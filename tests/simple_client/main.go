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

const signaturePrefix = "/* sentio-sig:"
const signatureSuffix = " */ "

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

	// Pre-calculate signed query
	originalQuery := "SELECT 1"
	hash := crypto.Keccak256Hash([]byte(originalQuery))
	sig, err := crypto.Sign(hash.Bytes(), key)
	if err != nil {
		log.Fatal(err)
	}
	sigHex := "0x" + hex.EncodeToString(sig)
	// Prepend signature as SQL comment
	signedQuery := signaturePrefix + sigHex + signatureSuffix + originalQuery


	for i := 0; i < total; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			defer func() { <-sem }()
			
			if err := runSingleRequest(addr, signedQuery); err != nil {
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

func runSingleRequest(addr string, signedQuery string) error {
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
		ProtocolVersion: 54460,
		Database:        "default",
		User:            "default",
	}
	var buf proto.Buffer
	info.Encode(&buf)
	if _, err := conn.Write(buf.Buf); err != nil {
		return fmt.Errorf("write hello: %w", err)
	}

	// 2. Read ServerHello response (wait for server to respond before sending Query)
	conn.SetReadDeadline(time.Now().Add(5*time.Second))
	helloBuf := make([]byte, 1024)
	n, err := conn.Read(helloBuf)
	if err != nil {
		return fmt.Errorf("read server hello: %w", err)
	}
	if n == 0 {
		return fmt.Errorf("empty server hello response")
	}

	// 3. Send Addendum (required by protocol after ServerHello)
	buf.Reset()
	// QuotaKey (string)
	if proto.FeatureQuotaKey.In(54460) {
		buf.PutString("") // empty quota key
	}
	// ChunkedSend (string)
	if proto.FeatureChunkedPackets.In(54460) {
		buf.PutString("") // empty chunked send
		buf.PutString("") // empty chunked recv
	}
	// ParallelReplicasVersion (uvarint)
	if proto.FeatureVersionedParallelReplicas.In(54460) {
		buf.PutUVarInt(0)
	}
	if _, err := conn.Write(buf.Buf); err != nil {
		return fmt.Errorf("write addendum: %w", err)
	}

	// 4. Send Query packet
	buf.Reset()
	
	// Packet Type: Query=1
	buf.PutUVarInt(1)
	
	// Query ID
	buf.PutString("")
	
	// Client Info
	clientInfo := proto.ClientInfo{
		ProtocolVersion: 54460,
		ClientName:      "SimpleClient",
		Interface:       1, // TCP
	}
	clientInfo.EncodeAware(&buf, 54460)
	
	// Settings (empty)
	buf.PutString("")

	// Interserver Roles (empty)
	if proto.FeatureInterserverExternallyGrantedRoles.In(54460) {
		buf.PutString("")
	}
	
	// Interserver Secret (empty)
	if proto.FeatureInterServerSecret.In(54460) {
		buf.PutString("")
	}
	
	// Stage = 2 (Complete)
	buf.PutUVarInt(2)
	
	// Compression = 0
	buf.PutUVarInt(0)
	
	// Query Body with signature prefix
	buf.PutString(signedQuery)
	
	// Parameters (empty)
	if proto.FeatureParameters.In(54460) {
		buf.PutString("")
	}
	
	if _, err := conn.Write(buf.Buf); err != nil {
		return fmt.Errorf("write query: %w", err)
	}

	// 5. Read response (EndOfStream = byte 5)
	conn.SetReadDeadline(time.Now().Add(5*time.Second))
	respBuf := make([]byte, 128)
	n, err = conn.Read(respBuf)
	// Accept as success if we read at least 1 byte (EndOfStream)
	if n > 0 {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	
	return nil
}
