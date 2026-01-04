package main

import (
	"crypto/ecdsa"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ethereum/go-ethereum/crypto"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:19000", "Proxy address")
	privKeyHex := flag.String("priv", "", "Private key hex")
	concurrency := flag.Int("c", 1, "Concurrency")
	total := flag.Int("n", 1, "Total requests")
	flag.Parse()

	if *privKeyHex == "" {
		log.Fatal("Private key required")
	}

	privKey, err := crypto.HexToECDSA(*privKeyHex)
	if err != nil {
		log.Fatalf("Invalid private key: %v", err)
	}

	runLoadTest(*addr, privKey, *concurrency, *total)
}

func runLoadTest(addr string, key *ecdsa.PrivateKey, concurrency int, total int) {
	fmt.Printf("Starting load test: concurrency=%d, total=%d\n", concurrency, total)
	
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	start := time.Now()
	var success, failures int64

	// Pre-calculate signature
	query := "SELECT 1"
	hash := crypto.Keccak256Hash([]byte(query))
	sig, err := crypto.Sign(hash.Bytes(), key)
	if err != nil {
		log.Fatal(err)
	}
	sigHex := "0x" + hex.EncodeToString(sig)


	for i := 0; i < total; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() { <-sem }()
			
			if err := runSingleRequest(addr, sigHex, query); err != nil {
				atomic.AddInt64(&failures, 1)
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

func runSingleRequest(addr string, sigHex string, query string) error {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	// 1. Send ClientHello
	info := proto.ClientHello{
		Name:            "SimpleClient",
		Major:           1,
		Minor:           1,
		ProtocolVersion: 54460, // Match proxy expectation
		Database:        "default",
		User:            "default",
	}
	var buf proto.Buffer
	info.Encode(&buf)
	if _, err := conn.Write(buf.Buf); err != nil {
		return err
	}

	// 2. Mock Server (or Proxy) might send ServerHello.
	// We don't read it to speed up (TCP buffering handles it).
	// If the protocol requires strict read-before-write, we might need to Read.
	// But usually ClientHello -> (async ServerHello) -> Query is fine.
    
    buf.Reset()
    
    // Packet Type: Query=1
    buf.PutUVarInt(1)
    
    // Query ID
    buf.PutString("")
    
    // Client Info with QuotaKey
    clientInfo := proto.ClientInfo{
        ProtocolVersion: 54460,
        ClientName:      "SimpleClient",
        QuotaKey:        sigHex,
    }
    clientInfo.EncodeAware(&buf, 54460)
    
	// Settings (empty)
    (proto.Setting{}).Encode(&buf)

    // Interserver Roles (empty)
    buf.PutString("")
    
    // Interserver Secret (empty)
    buf.PutString("")
    
    // Stage = 2 (Complete)
    buf.PutUVarInt(2)
    
    // Compression = 0
    buf.PutUVarInt(0)
    
    // Query Body
    buf.PutString(query)
    
    // Parameters (empty)
    (proto.Parameter{}).Encode(&buf)
    
    if _, err := conn.Write(buf.Buf); err != nil {
        return err
    }

	// 4. Expect response
    // Read 1 byte
    conn.SetReadDeadline(time.Now().Add(5*time.Second))
    b := make([]byte, 1)
    if _, err := conn.Read(b); err != nil {
        return err
    }
    
	return nil
}
