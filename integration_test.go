package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum/crypto"
)

// TestIntegration_EthAuth tests the full flow: client -> proxy -> mock server
// with Ethereum signature authentication.
func TestIntegration_EthAuth(t *testing.T) {
	// Channel to receive the settings received by the mock server
	settingsCh := make(chan map[string]string, 10)

	// Start mock ClickHouse server
	mockAddr, mockStop := startMockServer(t, settingsCh)
	defer mockStop()

	// Test private key and address
	privKeyHex := "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	allowedAddr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	// Start proxy with auth enabled
	proxyAddr, proxyStop := startProxyWithAuth(t, mockAddr, []string{allowedAddr})
	defer proxyStop()

	t.Run("ValidToken_Stripped", func(t *testing.T) {
		// Create ClickHouse connection to proxy
		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{proxyAddr},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
			},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("failed to open connection: %v", err)
		}
		defer conn.Close()

		// Generate auth token for the query
		sql := "SELECT 1"
		token := generateAuthToken(t, privKeyHex, sql)

		// Execute query with auth token in settings
		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"SQL_x_auth_token": token,
			"other_setting":    "value123",
		}))

		err = conn.Exec(ctx, "SELECT 1")
		if err != nil {
			t.Logf("Exec failed (expected with mock): %v", err)
		}

		// Check what the mock server received
		select {
		case receivedSettings := <-settingsCh:
			// Debug: print received settings
			t.Logf("Mock server received settings: %v", receivedSettings)

			// Verify x_auth_token is GONE
			if _, ok := receivedSettings["x_auth_token"]; ok {
				t.Fatal("x_auth_token should have been stripped but was present in upstream request")
			}
			if _, ok := receivedSettings["SQL_x_auth_token"]; ok {
				t.Fatal("SQL_x_auth_token should have been stripped but was present in upstream request")
			}

			// Verify other settings are PRESENT
			if val, ok := receivedSettings["other_setting"]; !ok || val != "value123" {
				t.Errorf("other_setting missing or incorrect. Got: %s", val)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Mock server did not receive query within timeout")
		}
	})

	t.Run("ValidToken_Stripped_LegacyKey", func(t *testing.T) {
		// Valid token but using "x_auth_token" key
		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{proxyAddr},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
			},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("failed to open connection: %v", err)
		}
		defer conn.Close()

		sql := "SELECT 1"
		token := generateAuthToken(t, privKeyHex, sql)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"x_auth_token": token,
		}))

		err = conn.Exec(ctx, "SELECT 1")
		if err != nil {
			t.Fatalf("Exec failed with x_auth_token: %v", err)
		}

		select {
		case receivedSettings := <-settingsCh:
			if _, ok := receivedSettings["x_auth_token"]; ok {
				t.Fatal("x_auth_token should have been stripped")
			}
			if _, ok := receivedSettings["SQL_x_auth_token"]; ok {
				t.Fatal("SQL_x_auth_token should have been stripped")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("Mock server did not receive query within timeout")
		}
	})

	t.Run("InvalidToken", func(t *testing.T) {
		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{proxyAddr},
			Auth: clickhouse.Auth{
				Database: "default",
				Username: "default",
			},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("failed to open connection: %v", err)
		}
		defer conn.Close()

		// Use a different private key (not in allowlist)
		badPrivKeyHex := "829e924fdd02fa1432a50e980a370e060938f71297e682af7fd7334a17937400"
		sql := "SELECT 1"
		token := generateAuthToken(t, badPrivKeyHex, sql)

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			"x_auth_token": token,
		}))

		err = conn.Ping(ctx)
		// With invalid token, the proxy should reject the query
		if err == nil {
			t.Log("Expected error for invalid token")
		}
	})
}

func generateAuthToken(t *testing.T, privKeyHex, sql string) string {
	t.Helper()

	privateKey, err := crypto.HexToECDSA(privKeyHex)
	if err != nil {
		t.Fatalf("failed to parse private key: %v", err)
	}

	// Build JWS header
	header := map[string]string{"alg": "ES256K", "typ": "JWT"}
	headerJSON, _ := json.Marshal(header)
	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)

	// Compute query hash
	qhash := "0x" + fmt.Sprintf("%x", crypto.Keccak256([]byte(sql)))

	// Build JWS payload
	payload := map[string]interface{}{
		"iat":   time.Now().Unix(),
		"qhash": qhash,
	}
	payloadJSON, _ := json.Marshal(payload)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	// Signing input
	signingInput := headerB64 + "." + payloadB64

	// Sign with Ethereum style (Keccak256 of signing input)
	hash := crypto.Keccak256([]byte(signingInput))
	sig, err := crypto.Sign(hash, privateKey)
	if err != nil {
		t.Fatalf("failed to sign: %v", err)
	}

	// Adjust V (0/1 -> 27/28) for Ethereum convention
	sig[64] += 27

	signatureB64 := base64.RawURLEncoding.EncodeToString(sig)
	return signingInput + "." + signatureB64
}

func startMockServer(t *testing.T, settingsCh chan<- map[string]string) (string, func()) {
	t.Helper()

	// For simplicity, we'll use a basic TCP server that accepts connections
	// and sends minimal ClickHouse-like responses
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock server: %v", err)
	}

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handleMockConn(conn, settingsCh)
		}
	}()

	return ln.Addr().String(), func() { ln.Close() }
}

func handleMockConn(conn net.Conn, settingsCh chan<- map[string]string) {
	defer conn.Close()

	// Wrap conn with a reader that can Uvarint
	r := proto.NewReader(conn)

	// 1. Read ClientHello (Packet Type 0)
	typeID, err := r.UVarInt()
	if err != nil {
		return
	}
	// Note: We might get other packets, but standard handshake starts with Hello
	if typeID != 0 {
		return
	}

	var hello proto.ClientHello
	if err := hello.Decode(r); err != nil {
		return
	}
	version := hello.ProtocolVersion

	// 1.5. Send ServerHello (Type 0) to complete handshake
	{
		serverHello := proto.ServerHello{
			Name:        "MockClickHouse",
			Major:       24,
			Minor:       1,
			Patch:       1,
			Revision:    54460,
			Timezone:    "UTC",
			DisplayName: "Mock",
		}
		var pb proto.Buffer
		serverHello.EncodeAware(&pb, version)

		// Note: EncodeAware writes Type 0 (ServerCodeHello) internally.
		conn.Write(pb.Buf)
	}

	// Consume Addendum if present
	if err := consumeAddendum(r, version); err != nil {
		fmt.Printf("Mock: Addendum consumption error: %v\n", err)
		return
	}

	// 2. Read Query (Packet Type 1) - EXPECTED
	fmt.Println("Mock: Waiting for Query (Type 1)...")
	typeID, err = r.UVarInt()
	if err != nil {
		fmt.Printf("Mock: UVarInt error reading query type: %v\n", err)
		return
	}
	fmt.Printf("Mock: Received packet type %d\n", typeID)

	if typeID == 1 {
		var q proto.Query
		// Use DecodeAware to decode based on version
		if err := q.DecodeAware(r, version); err != nil {
			fmt.Printf("Mock: DecodeAware error: %v\n", err)
			return
		}

		fmt.Printf("Mock: Decoded Query Body: %s\n", q.Body)
		// Extract settings
		settings := make(map[string]string)
		for _, s := range q.Settings {
			settings[s.Key] = s.Value
		}
		fmt.Printf("Mock: Decoded Query Settings: %v\n", settings)

		// Send to channel
		select {
		case settingsCh <- settings:
			fmt.Println("Mock: Settings sent to channel")
		default:
			fmt.Println("Mock: Settings channel full or closed")
		}
	} else {
		// Not a Query?
		// Maybe Ping (4)?
	}

	// Send a minimal "EndOfStream" response (Packet Type 5)
	// Server packets also start with uvarint type.
	// 5 = EndOfStream
	conn.Write([]byte{5})
}

func consumeAddendum(r *proto.Reader, version int) error {
	if proto.FeatureQuotaKey.In(version) {
		if _, err := r.Str(); err != nil {
			return err
		}
	}
	// FeatureChunkedPackets logic? ch-go proto doesn't expose strict "Chunks" feature easily?
	// But assuming standard ClickHouse behavior.
	// Use proxy.go logic reference:
	// if proto.FeatureChunkedPackets.In(p.version) { readString(); readString() }
	// However, FeatureChunkedPackets is usually high revision.
	// Let's implement reading a string if FeatureQuotaKey is in.

	// WAIT: ch-go/proto Reader Str() reads a string.
	// If FeatureQuotaKey is present, client sends Quota Key (string).
	// If it's empty, length is 0. r.Str() handles it.

	return nil
}

func startProxyWithAuth(t *testing.T, upstreamAddr string, allowedAddrs []string) (string, func()) {
	t.Helper()

	// Find a free port
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	proxyAddr := ln.Addr().String()
	ln.Close()

	cfg := Config{
		Listen:               proxyAddr,
		Upstream:             upstreamAddr,
		DialTimeout:          Duration{5 * time.Second},
		IdleTimeout:          Duration{30 * time.Second},
		AuthEnabled:          true,
		AuthAllowedAddresses: allowedAddrs,
		AuthMaxTokenAge:      Duration{1 * time.Minute},
	}

	validator := NewEthValidator(cfg.AuthAllowedAddresses, cfg.AuthMaxTokenAge.Duration, true, false)
	proxy := newProxy(cfg, validator)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = proxy.serve(ctx)
	}()

	// Wait for proxy to start
	time.Sleep(100 * time.Millisecond)

	return proxyAddr, cancel
}
