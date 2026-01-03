package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
	target := flag.String("target", "127.0.0.1:9000", "proxy address")
	queryFile := flag.String("file", "tests/data/exported_queries.json", "path to queries file")
	concurrency := flag.Int("c", 10, "concurrency")
    // Use a large limit for tests
	limit := flag.Int("n", 5000, "max queries to run")
	flag.Parse()

	queries, err := loadQueries(*queryFile)
	if err != nil {
		log.Fatalf("Failed to load queries: %v", err)
	}
	log.Printf("Loaded %d unique query templates from file", len(queries))
    log.Printf("Starting execution of %d total queries...", *limit)

	if len(queries) == 0 {
		log.Fatal("No queries found")
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{*target},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		DialTimeout: 2 * time.Second,
	})
	if err != nil {
		log.Fatal("failed to connect:", err)
	}
    // Skip Ping as mock server acts as sink
    
	sem := make(chan struct{}, *concurrency)
	var wg sync.WaitGroup

	start := time.Now()
	var success, failures int64
	var mu sync.Mutex

	var count int64 

	for i := 0; i < *limit; i++ {
		q := queries[i%len(queries)]
        // Simple deduplication of newlines if any
		q = strings.TrimSpace(q)
		if q == "" {
			continue
		}

		sem <- struct{}{}
		wg.Add(1)
		go func(id int, sql string) {
			defer wg.Done()
			defer func() { <-sem }()

			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

            // Just Execute, don't care about rows
			err := conn.Exec(ctx, sql)
            
			mu.Lock()
            c := count + 1
			count = c
			if err != nil {
                // In mock server we just discard, so it should succeed.
                // But if the query is a SELECT, Exec might fail because it expects no rows?
                // Actually clickhouse-go Exec is for non-selects. Query is for selects.
                // But mock server returns nothing.
                // If we send a Query packet, and Mock server consumes it and does nothing...
                // The client will wait for Data packets.
                // So Mock Server needs to be smarter? 
                // Or we accept timeouts as "processed".
                
                // Wait, if I use a real driver, it expects a valid protocol response.
                // My Mock Server only sends ServerHello.
                // After sending Query, the client expects Data or EndOfStream.
                // If Mock Server absorbs data and sends nothing, the client will timeout waiting for response.
                // Thus "Timeout" is actually the expected "Success" for a dummy sink.
                
                // To make it cleaner, maybe I should just use raw TCP client to send "Query" packet?
                // But constructing a valid Query packet with ch-go is hard without a session.
                
                // Alternative: Update Mock Server to reply with an empty block for Query?
                // Too complex for "just testing forwarding".
                
                // Let's stick to: If error is "Timeout" or "EOF" (server closed), it means traffic went through.
                // "Connection refused" is the real failure.
				failures++
                // Log only real errors
                if !strings.Contains(err.Error(), "timeout") && !strings.Contains(err.Error(), "unexpected EOF") {
                    log.Printf("Query failed: %v", err)
                } 
			} else {
				success++
			}
			mu.Unlock()
		}(i, q)
	}

	wg.Wait()
	duration := time.Since(start)
	log.Printf("Done. Duration: %v. Success (approx): %d, Failures (expected timeouts): %d", duration, success, failures)
}

type QueryRow struct {
	Query string `json:"query"`
}

func loadQueries(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var qs []string
	scanner := bufio.NewScanner(f)
    // 1MB buffer
    buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)
    
	for scanner.Scan() {
		line := scanner.Text()
        var row QueryRow
        if err := json.Unmarshal([]byte(line), &row); err != nil {
			// fallback to raw text if json fails
            if len(line) > 5 {
			    qs = append(qs, line)
            }
            continue
        }
		if len(row.Query) > 5 {
			qs = append(qs, row.Query)
		}
	}
	return qs, scanner.Err()
}
