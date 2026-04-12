//go:build ignore
// +build ignore

package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/sentioxyz/clickhouse-proxy/pkg/proxy"
)

func main() {
	// Load real network state from YAML (exported from Redis state mirror)
	state, err := proxy.LoadNetworkStateFromYAML("configs/test_network_state.yaml")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load network state: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("✅ Network state loaded successfully")

	// Print state info
	allocs := state.ProcessorAllocations
	for pid, m := range allocs {
		for iid, alloc := range m {
			fmt.Printf("   ProcessorAllocation: pid=%s → indexerId=%d\n", pid, iid)
			_ = alloc
		}
	}
	for iid, info := range state.IndexerInfos {
		fmt.Printf("   IndexerInfo: id=%d, url=%s, proxyPort=%d\n", iid, info.IndexerUrl, info.ClickhouseProxyPort)
	}
	for pid, pinfo := range state.ProcessorInfos {
		schema := pinfo.EntitySchema
		if len(schema) > 80 {
			schema = schema[:80] + "..."
		}
		fmt.Printf("   ProcessorInfo: pid=%s, schema=%s\n", pid, schema)
	}

	// Create rewriter with real gRPC connection to sql-rewriter
	config := proxy.RewriterConfig{
		Enabled:        true,
		ServiceAddr:    "localhost:50051", // port-forwarded sql-rewriter
		LocalIndexerId: 5,                 // same as real indexerId, so tables are "local"
		CHUser:         "default",
		CHPassword:     "",
		Timeout:        10 * time.Second,
	}

	rewriter, err := proxy.NewSentioNetworkRewriter(config, state, proxy.DefaultTableRewriterFactory())
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create rewriter: %v\n", err)
		os.Exit(1)
	}
	defer rewriter.Close()
	fmt.Println("✅ Rewriter created with gRPC connection to localhost:50051")

	// Test SQL queries
	testCases := []struct {
		name string
		sql  string
	}{
		{
			name: "Local table query (processorId=dvBsZtMc)",
			sql:  "SELECT * FROM sentio_dvBsZtMc.GasSpent LIMIT 10",
		},
		{
			name: "No Sentio-Network table",
			sql:  "SELECT 1",
		},
		{
			name: "Unknown processor",
			sql:  "SELECT * FROM sentio_unknown.SomeTable",
		},
	}

	ctx := context.Background()
	fmt.Println("\n--- Running E2E tests ---\n")

	for _, tc := range testCases {
		fmt.Printf("📋 Test: %s\n", tc.name)
		fmt.Printf("   Input:  %s\n", tc.sql)

		result, err := rewriter.Rewrite(ctx, tc.sql)
		if err != nil {
			fmt.Printf("   ❌ Error: %v\n", err)
		} else {
			fmt.Printf("   Output: %s\n", result)
			if result != tc.sql {
				fmt.Printf("   ✅ SQL was rewritten\n")
			} else {
				fmt.Printf("   ℹ️  SQL unchanged\n")
			}
		}
		fmt.Println()
	}
}
