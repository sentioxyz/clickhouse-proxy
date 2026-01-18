package main

import (
	"context"
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/sha3"
)

const (
	// AuthTokenSettingKey is the setting key used to pass the JWS authentication token.
	AuthTokenSettingKey = "x_auth_token"

	// CorrectPrivateKeyHex corresponds to address included in auth_ck.yaml
	// Address: 0x2932A8aAd29e41b90A447E586651587bea3eB11E
	CorrectPrivateKeyHex = "e7bc94e4a2346bfb31ce777e079044718ed02d53d8c297c69fce4259e96557bd"

	// WrongPrivateKeyHex corresponds to an address NOT in auth_ck.yaml
	// Generated randomly
	WrongPrivateKeyHex = "1111111111111111111111111111111111111111111111111111111111111111"

	TestDatabase = "auth_test_db"
	TestTable    = "auth_test_table"
)

// JWSHeader represents the header of a JWS token.
type JWSHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// JWSPayload represents the payload of a JWS authentication token.
type JWSPayload struct {
	Iat       int64  `json:"iat"`
	QueryHash string `json:"qhash"`
}

func main() {
	// Command line flags
	addr := flag.String("addr", "127.0.0.1:9002", "ClickHouse proxy address (default 9002)")
	user := flag.String("user", "sentio", "ClickHouse username")
	pass := flag.String("password", "2vwzZBJ6cbZbyoKm4j", "ClickHouse password")
	flag.Parse()

	log.Println("===========================================")
	log.Println("  ClickHouse Auth Test Client")
	log.Println("===========================================")
	log.Printf("Target: %s", *addr)
	log.Printf("User: %s", *user)

	// Pre-parse keys
	correctKey, err := crypto.HexToECDSA(CorrectPrivateKeyHex)
	if err != nil {
		log.Fatalf("Failed to parse correct private key: %v", err)
	}
	wrongKey, err := crypto.HexToECDSA(WrongPrivateKeyHex)
	if err != nil {
		log.Fatalf("Failed to parse wrong private key: %v", err)
	}

	// Connect to ClickHouse
	// Note: We don't put settings in the base connection, but in the context per query.
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{*addr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: *user,
			Password: *pass,
		},
		DialTimeout:     10 * time.Second,
		ConnMaxLifetime: time.Hour,
	})
	if err != nil {
		log.Fatalf("Failed to open connection: %v", err)
	}
	defer conn.Close()

	if err := conn.Ping(context.Background()); err != nil {
		log.Printf("Warning: Ping failed: %v", err)
	} else {
		log.Println("Ping successful")
	}
	log.Println()

	// Define Test Scenarios
	scenarios := []struct {
		Name          string
		Key           *ecdsa.PrivateKey
		NoKey         bool
		ExpectSuccess bool
	}{
		{
			Name:          "Scenario 1: Correct JWK (Should Succeed)",
			Key:           correctKey,
			NoKey:         false,
			ExpectSuccess: true,
		},
		{
			Name:          "Scenario 2: Wrong JWK (Should Fail)",
			Key:           wrongKey,
			NoKey:         false,
			ExpectSuccess: false,
		},
		{
			Name:          "Scenario 3: No JWK (Should Fail)",
			Key:           nil,
			NoKey:         true,
			ExpectSuccess: false,
		},
	}

	for _, s := range scenarios {
		runScenario(conn, s.Name, s.Key, s.NoKey, s.ExpectSuccess)
		log.Println("-------------------------------------------")
	}

	log.Println("All test scenarios completed.")
}

func runScenario(conn clickhouse.Conn, name string, key *ecdsa.PrivateKey, noKey bool, expectSuccess bool) {
	log.Printf(">>> Running %s", name)

	queries := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", TestDatabase),
		fmt.Sprintf("CREATE TABLE %s.%s (id Int64, val String) ENGINE = Memory", TestDatabase, TestTable),
		fmt.Sprintf("INSERT INTO %s.%s VALUES (1, 'test')", TestDatabase, TestTable),
		fmt.Sprintf("SELECT * FROM %s.%s", TestDatabase, TestTable),
		fmt.Sprintf("DROP TABLE %s.%s", TestDatabase, TestTable),
		fmt.Sprintf("DROP DATABASE %s", TestDatabase),
	}

	for i, q := range queries {
		log.Printf("[Step %d] Executing: %s", i+1, q)

		var ctx context.Context
		if noKey {
			ctx = context.Background()
		} else {
			token, err := createJWSToken(key, q)
			if err != nil {
				log.Fatalf("Failed to create token: %v", err)
			}
			ctx = clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
				AuthTokenSettingKey: clickhouse.CustomSetting{Value: token},
			}))
		}

		err := conn.Exec(ctx, q)
		if expectSuccess {
			if err != nil {
				log.Fatalf("❌ FAILED: Expected success but got error: %v", err)
			}
			log.Println("   ✅ Success")
		} else {
			if err == nil {
				log.Fatalf("❌ FAILED: Expected error but got success!")
			}
			log.Printf("   ✅ Got expected error: %v", err)
			// If we expected failure and got it, we stop the sequence here because
			// subsequent steps (like INSERT) depend on previous ones (like CREATE)
			// passing. It doesn't make sense to try to DROP a table we failed to CREATE.
			log.Println("   Stopping remaining steps for this failure scenario.")
			return
		}
	}
	log.Printf(">>> %s Passed\n", name)
}

func createJWSToken(privateKey *ecdsa.PrivateKey, query string) (string, error) {
	// Create header
	header := JWSHeader{
		Alg: "ES256K",
		Typ: "JWS",
	}
	headerBytes, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("failed to marshal header: %w", err)
	}
	headerB64 := base64.RawURLEncoding.EncodeToString(headerBytes)

	// Create payload with query hash
	queryHash := keccak256Hex([]byte(query))
	payload := JWSPayload{
		Iat:       time.Now().Unix(),
		QueryHash: queryHash,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadBytes)

	// Create signing input
	signingInput := headerB64 + "." + payloadB64

	// Sign the message hash
	messageHash := keccak256([]byte(signingInput))

	// Sign with recoverable signature (65 bytes: R || S || V)
	sig, err := crypto.Sign(messageHash, privateKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign: %w", err)
	}

	// Adjust V (0/1 -> 27/28) for Ethereum convention
	sig[64] += 27

	sigB64 := base64.RawURLEncoding.EncodeToString(sig)

	return signingInput + "." + sigB64, nil
}

// keccak256 computes the Keccak256 hash of the input.
func keccak256(data []byte) []byte {
	h := sha3.NewLegacyKeccak256()
	h.Write(data)
	return h.Sum(nil)
}

// keccak256Hex computes the Keccak256 hash and returns it as a hex string with 0x prefix.
func keccak256Hex(data []byte) string {
	return "0x" + hex.EncodeToString(keccak256(data))
}
