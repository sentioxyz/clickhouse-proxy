package main

import (
	"context"
	"io"
	"log"
	"os"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

func mustExec(ctx context.Context, conn ch.Conn, sql string, args ...any) {
	if err := conn.Exec(ctx, sql, args...); err != nil {
		log.Fatalf("exec %q failed: %v", sql, err)
	}
}

func main() {
	ctx := context.Background()

	// Log to stdout and file for inspection.
	_ = os.MkdirAll(".ck_runtime", 0o755)
	f, err := os.OpenFile(".ck_runtime/demo.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err == nil {
		log.SetOutput(io.MultiWriter(os.Stdout, f))
		defer f.Close()
	}

	// Proxy endpoints
	addrA := "127.0.0.1:9001"
	addrB := "127.0.0.1:9002"

	// Connect to A and B through their respective proxies.
	connA, err := ch.Open(&ch.Options{
		Addr: []string{addrA},
		Auth: ch.Auth{Database: "default", Username: "default"},
	})
	if err != nil {
		log.Fatalf("connect A via proxy: %v", err)
	}
	defer connA.Close()

	connB, err := ch.Open(&ch.Options{
		Addr: []string{addrB},
		Auth: ch.Auth{Database: "default", Username: "default"},
	})
	if err != nil {
		log.Fatalf("connect B via proxy: %v", err)
	}
	defer connB.Close()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Prepare tables on A and B.
	mustExec(ctx, connA, "DROP TABLE IF EXISTS t_a")
	mustExec(ctx, connA, "CREATE TABLE t_a (id UInt32) ENGINE=Memory")
	mustExec(ctx, connA, "INSERT INTO t_a VALUES (1),(2),(3)")

	mustExec(ctx, connB, "DROP TABLE IF EXISTS t_b")
	mustExec(ctx, connB, "CREATE TABLE t_b (id UInt32) ENGINE=Memory")
	mustExec(ctx, connB, "INSERT INTO t_b VALUES (5),(7)")

	// Local query on B.
	var sumB uint64
	if err := connB.QueryRow(ctx, "SELECT sum(id) FROM t_b").Scan(&sumB); err != nil {
		log.Fatalf("query local sum on B: %v", err)
	}
	log.Printf("sum on B = %d", sumB)

	// Remote query on B -> A through both proxies.
	var sumA uint64
	remoteSQL := "SELECT sum(id) FROM remote(?, 'default', 't_a', 'default', '')"
	if err := connB.QueryRow(ctx, remoteSQL, addrA).Scan(&sumA); err != nil {
		log.Fatalf("remote query B -> A failed: %v", err)
	}
	log.Printf("remote sum from A via proxy = %d", sumA)
}
