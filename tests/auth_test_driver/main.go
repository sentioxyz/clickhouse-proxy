package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum/crypto"
)

const signaturePrefix = "/* sentio-sig:"
const signatureSuffix = " */ "

func main() {
	addr := flag.String("addr", "127.0.0.1:19000", "Proxy address")
	privKeyHex := flag.String("priv", "", "Private key hex (without 0x)")
	concurrency := flag.Int("c", 1, "Concurrency level")
	total := flag.Int("n", 1, "Total requests")
	flag.Parse()

	if *privKeyHex == "" {
		log.Fatal("Private key required")
	}

	privKey, err := crypto.HexToECDSA(*privKeyHex)
	if err != nil {
		log.Fatalf("Invalid private key: %v", err)
	}

	fmt.Printf("Testing against %s with key %s\n", *addr, crypto.PubkeyToAddress(privKey.PublicKey).Hex())

	if *total > 1 || *concurrency > 1 {
		runLoadTest(*addr, privKey, *concurrency, *total)
		return
	}

	if err := runTest(*addr, privKey, true); err != nil {
		log.Fatalf("Test with valid signature failed: %v", err)
	}
	fmt.Println("✅ Valid signature test passed")

	// Test with NO signature (should fail if auth is enabled)
	if err := runTest(*addr, nil, false); err == nil {
		log.Fatalf("Test with missing signature succeeded but should have failed")
	} else {
		fmt.Printf("✅ Missing signature test passed (failed as expected: %v)\n", err)
	}
	
	// Test with INVALID signature
	invalidKey, _ := crypto.GenerateKey()
	if err := runTest(*addr, invalidKey, false); err == nil {
		log.Fatalf("Test with invalid signature succeeded but should have failed")
	} else {
		fmt.Printf("✅ Invalid signature test passed (failed as expected: %v)\n", err)
	}
}

func runLoadTest(addr string, key *ecdsa.PrivateKey, concurrency int, total int) {
	fmt.Printf("Starting load test: concurrency=%d, total=%d\n", concurrency, total)
	
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	start := time.Now()
	var success, failures int64
	
	for i := 0; i < total; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() { <-sem }()
			
			if err := runTest(addr, key, true); err != nil {
				atomic.AddInt64(&failures, 1)
				// log sampling
				if atomic.LoadInt64(&failures) < 50 { 
					log.Printf("Req %d failed: %v", id, err)
				}
			} else {
				atomic.AddInt64(&success, 1)
			}
		}(i)
	}
	wg.Wait()
	duration := time.Since(start)
	qps := float64(total) / duration.Seconds()
	fmt.Printf("Load Test Done. Duration: %v. QPS: %.2f. Success: %d. Failures: %d\n", duration, qps, success, failures)
}

func runTest(addr string, key *ecdsa.PrivateKey, expectSuccess bool) error {
	opts := clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		DialTimeout: 30 * time.Second,
	}
	
	conn, err := clickhouse.Open(&opts)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}

	ctx := context.Background()
	originalQuery := "SELECT 1"

	// Build query with signature prefix comment
	var query string
	if key != nil {
		// Sign the original query body
		hash := crypto.Keccak256Hash([]byte(originalQuery))
		sig, err := crypto.Sign(hash.Bytes(), key)
		if err != nil {
			return fmt.Errorf("sign: %w", err)
		}
		sigHex := "0x" + hex.EncodeToString(sig)
		// Prepend signature as SQL comment
		query = signaturePrefix + sigHex + signatureSuffix + originalQuery
	} else {
		query = originalQuery
	}

	var result int
	err = conn.QueryRow(ctx, query).Scan(&result)
	return err
}
