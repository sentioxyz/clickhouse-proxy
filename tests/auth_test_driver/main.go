package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum/crypto"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:19000", "Proxy address")
	privKeyHex := flag.String("priv", "", "Private key hex (without 0x)")
	flag.Parse()

	if *privKeyHex == "" {
		log.Fatal("Private key required")
	}

	privKey, err := crypto.HexToECDSA(*privKeyHex)
	if err != nil {
		log.Fatalf("Invalid private key: %v", err)
	}

	fmt.Printf("Testing against %s with key %s\n", *addr, crypto.PubkeyToAddress(privKey.PublicKey).Hex())

	if err := runTest(*addr, privKey, true); err != nil {
		log.Fatalf("Test with valid signature failed: %v", err)
	}
	fmt.Println("✅ Valid signature test passed")

	// Test with NO signature (should fail if auth is enabled)
	// We need a separate connection for this as QuotaKey might be cached or we need strict separation
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
	
	start := time.Now()
	fmt.Printf("[%t] Connecting... ", expectSuccess)
	conn, err := clickhouse.Open(&opts)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	fmt.Printf("Connected in %v.\n", time.Since(start))

	ctx := context.Background()
	query := "SELECT 1"

	var queryOpts []interface{}
	
	if key != nil {
		// Sign the query
		hash := crypto.Keccak256Hash([]byte(query))
		sig, err := crypto.Sign(hash.Bytes(), key)
		if err != nil {
			return fmt.Errorf("sign: %w", err)
		}
		sigHex := "0x" + hex.EncodeToString(sig)
		queryOpts = append(queryOpts, clickhouse.WithQuotaKey(sigHex))
	}

	// We expect the proxy to forward this to mock server, mock server returns "1" presumably or we just check connectivity
	// Actually, relying on mock server response might be tricky if mock server doesn't respond to SELECT 1
	// checking mock_server code would be good. But for now we check if Proxy REJECTS the connection.
	
	// If proxy rejects, it drops connection, so we get EOF or connection closed.
	
	var result int
	err = conn.QueryRow(ctx, query, queryOpts...).Scan(&result)
	return err
}
