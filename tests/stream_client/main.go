package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	source := flag.String("source", "127.0.0.1:9001", "source ClickHouse address")
	target := flag.String("target", "127.0.0.1:19000", "target proxy address")
	concurrency := flag.Int("c", 50, "concurrency (number of TCP connections)")
	limit := flag.Int("n", 0, "max queries (0 = all)")
	since := flag.String("since", "1 hour", "time range")
	mode := flag.String("mode", "fast", "mode: 'fast' (raw TCP, no response) or 'verify' (full protocol)")
	flag.Parse()

	log.Printf("Stream Replay: source=%s target=%s concurrency=%d mode=%s", *source, *target, *concurrency, *mode)

	// Fetch queries from source
	queries := fetchQueries(*source, *since, *limit)
	if len(queries) == 0 {
		log.Println("No queries to replay")
		return
	}
	log.Printf("Loaded %d queries", len(queries))

	if *mode == "fast" {
		runFastMode(queries, *target, *concurrency)
	} else {
		runVerifyMode(queries, *target, *concurrency)
	}
}

func fetchQueries(source, since string, limit int) []string {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{source},
		Auth: clickhouse.Auth{Database: "system", Username: "default", Password: ""},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect to source: %v", err)
	}
	defer conn.Close()

	// Get count
	var total uint64
	countQuery := fmt.Sprintf(`SELECT count() FROM system.query_log 
		WHERE type = 'QueryFinish' AND query NOT LIKE '%%system%%' AND query NOT LIKE '%%query_log%%'
		AND event_time > now() - INTERVAL %s`, since)
	if err := conn.QueryRow(context.Background(), countQuery).Scan(&total); err != nil {
		log.Fatalf("Failed to count: %v", err)
	}

	actualLimit := total
	if limit > 0 && uint64(limit) < total {
		actualLimit = uint64(limit)
	}
	log.Printf("Found %d queries, will replay %d", total, actualLimit)

	// Fetch
	fetchQuery := fmt.Sprintf(`SELECT query FROM system.query_log 
		WHERE type = 'QueryFinish' AND query NOT LIKE '%%system%%' AND query NOT LIKE '%%query_log%%'
		AND event_time > now() - INTERVAL %s ORDER BY event_time DESC LIMIT %d`, since, actualLimit)
	
	rows, err := conn.Query(context.Background(), fetchQuery)
	if err != nil {
		log.Fatalf("Failed to fetch: %v", err)
	}

	var queries []string
	for rows.Next() {
		var q string
		if err := rows.Scan(&q); err != nil {
			continue
		}
		q = strings.TrimSpace(q)
		if len(q) > 10 {
			queries = append(queries, q)
		}
	}
	rows.Close()
	return queries
}

// Fast mode: raw TCP, fire-and-forget, just tests proxy forwarding
func runFastMode(queries []string, target string, concurrency int) {
	log.Println("Mode: FAST (raw TCP, fire-and-forget)")
	
	var processed, success, failures int64
	total := int64(len(queries))
	start := time.Now()

	// Progress goroutine
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p := atomic.LoadInt64(&processed)
				pct := float64(p) / float64(total) * 100
				qps := float64(p) / time.Since(start).Seconds()
				fmt.Printf("\r[Progress] %d/%d (%.1f%%) | QPS: %.0f | Elapsed: %v    ",
					p, total, pct, qps, time.Since(start).Round(time.Second))
			case <-done:
				return
			}
		}
	}()

	// Worker pool with persistent connections
	queryChan := make(chan string, 1000)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn := dialAndHandshake(target)
			if conn == nil {
				return
			}
			defer conn.Close()

			for q := range queryChan {
				if err := sendQuery(conn, q); err != nil {
					atomic.AddInt64(&failures, 1)
					// Reconnect on error
					conn.Close()
					conn = dialAndHandshake(target)
					if conn == nil {
						return
					}
				} else {
					atomic.AddInt64(&success, 1)
				}
				atomic.AddInt64(&processed, 1)
			}
		}()
	}

	// Feed queries
	for _, q := range queries {
		queryChan <- q
	}
	close(queryChan)
	wg.Wait()
	close(done)

	duration := time.Since(start)
	fmt.Println()
	log.Println("========================================")
	log.Println("          STREAM REPLAY COMPLETE        ")
	log.Println("========================================")
	log.Printf("Total:    %d queries", total)
	log.Printf("Duration: %v", duration)
	log.Printf("QPS:      %.0f", float64(total)/duration.Seconds())
	log.Printf("Success:  %d", success)
	log.Printf("Failures: %d", failures)
	log.Println("========================================")
	if failures == 0 {
		log.Println("✅ All queries forwarded!")
	} else {
		log.Printf("⚠️  %d queries failed", failures)
	}
}

func dialAndHandshake(target string) net.Conn {
	conn, err := net.DialTimeout("tcp", target, 2*time.Second)
	if err != nil {
		log.Printf("dial failed: %v", err)
		return nil
	}

	// Send minimal ClientHello using raw bytes
	// This is a simplified ClickHouse handshake that the proxy will forward
	var buf proto.Buffer
	buf.PutString("StreamClient") // client_name
	buf.PutUVarInt(22)             // version_major
	buf.PutUVarInt(8)              // version_minor
	buf.PutUVarInt(54460)          // client_revision
	buf.PutString("default")       // database
	buf.PutString("default")       // user
	buf.PutString("")              // password

	if _, err := conn.Write(buf.Buf); err != nil {
		conn.Close()
		return nil
	}

	// Read ServerHello (just consume it)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	tmp := make([]byte, 1024)
	conn.Read(tmp)
	conn.SetReadDeadline(time.Time{})

	return conn
}

func sendQuery(conn net.Conn, query string) error {
	var buf proto.Buffer
	
	// Client Query packet (type 1)
	buf.PutByte(1) // ClientCodeQuery
	buf.PutString("") // query_id
	
	// ClientInfo block (simplified)
	buf.PutByte(1)   // initial_user
	buf.PutString("") // initial_query_id
	buf.PutString("[::1]:0") // initial_address
	buf.PutByte(0)   // os_user
	buf.PutString("") 
	buf.PutString("stream_client")  // client_hostname
	buf.PutString("StreamClient")   // client_name
	buf.PutUVarInt(22) // version_major
	buf.PutUVarInt(8)  // version_minor
	buf.PutUVarInt(54460) // revision
	buf.PutString("")  // quota_key
	buf.PutUVarInt(0)  // distributed_depth
	buf.PutUVarInt(uint64(proto.Version)) // client version
	buf.PutByte(0)     // collaborate_with_initiator
	buf.PutUVarInt(0)  // count_participating_replicas
	buf.PutUVarInt(0)  // number_of_current_replica
	
	// Settings (empty)
	buf.PutString("")
	
	// Interserver secret (empty since revision)
	buf.PutString("")
	
	// State
	buf.PutUVarInt(2) // Complete
	
	// Compression
	buf.PutByte(0)
	
	// Query
	buf.PutString(query)

	conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	_, err := conn.Write(buf.Buf)
	conn.SetWriteDeadline(time.Time{})
	return err
}

// Verify mode: uses clickhouse-go, waits for response
func runVerifyMode(queries []string, target string, concurrency int) {
	log.Println("Mode: VERIFY (full protocol, slower)")
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{target},
		Auth: clickhouse.Auth{Database: "default", Username: "default", Password: ""},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	var processed, success, failures int64
	total := int64(len(queries))
	start := time.Now()

	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p := atomic.LoadInt64(&processed)
				pct := float64(p) / float64(total) * 100
				qps := float64(p) / time.Since(start).Seconds()
				fmt.Printf("\r[Progress] %d/%d (%.1f%%) | QPS: %.1f    ", p, total, pct, qps)
			case <-done:
				return
			}
		}
	}()

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, q := range queries {
		sem <- struct{}{}
		wg.Add(1)
		go func(sql string) {
			defer wg.Done()
			defer func() { <-sem }()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if err := conn.Exec(ctx, sql); err != nil {
				atomic.AddInt64(&failures, 1)
			} else {
				atomic.AddInt64(&success, 1)
			}
			atomic.AddInt64(&processed, 1)
		}(q)
	}

	wg.Wait()
	close(done)
	fmt.Println()
	log.Printf("Done. Success: %d, Failures: %d", success, failures)
}

type QueryRow struct {
	Query string `json:"query"`
}

func loadQueriesFromFile(path string) []string {
	f, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer f.Close()
	var qs []string
	scanner := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		var row QueryRow
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			if len(line) > 5 {
				qs = append(qs, line)
			}
			continue
		}
		if len(row.Query) > 5 {
			qs = append(qs, row.Query)
		}
	}
	return qs
}
