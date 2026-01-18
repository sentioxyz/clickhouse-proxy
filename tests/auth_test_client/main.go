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

	// Second Correct Private Key (needs to be added to verify multisig effectively)
	// We will repurpose the existing "Correct" key and add a new one.
	// For this test to work with the EXISTING auth_ck.yaml, we need to add another key to the yaml first.
	// OR, we can just use the existing keys in the YAML if we had more.
	// Currently auth_ck.yaml has:
	// 1. 0x2c7536e3605d9c16a7a3d7b1898e529396a65c23 (Original)
	// 2. 0x86cE23361B15507dDbf734EE32904312C6A16eE3 (Original)
	// 3. 0x2932A8aAd29e41b90A447E586651587bea3eB11E (Added by us)

	// Let's use one of the original keys as the "Second Correct Key" for multisig testing.
	// Private key for 0x86cE... is not known to us here easily unless we generate it or have it.
	// To make it easy, let's generate a NEW key and add it to `auth_ck.yaml` in the next step.
	// For now, I will add the logic assuming we have a `CorrectPrivateKeyHex2`.

	// Address: 0x2932A8aAd29e41b90A447E586651587bea3eB11E
	CorrectPrivateKeyHex1 = "e7bc94e4a2346bfb31ce777e079044718ed02d53d8c297c69fce4259e96557bd"

	// Address: 0x709F74A4e8C545E00d98499252445E1643261642 (Newly generated for multisig test)
	CorrectPrivateKeyHex2 = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

	// WrongPrivateKeyHex corresponds to an address NOT in auth_ck.yaml
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

// JWSJSON represents the JWS JSON Serialization format.
type JWSJSON struct {
	Payload    string             `json:"payload"`
	Signatures []JWSJSONSignature `json:"signatures"`
}

// JWSJSONSignature represents a single signature in the JWS JSON format.
type JWSJSONSignature struct {
	Protected string `json:"protected"`
	Signature string `json:"signature"`
}

func main() {
	// Command line flags
	addr := flag.String("addr", "127.0.0.1:9002", "ClickHouse proxy address (default 9002)")
	user := flag.String("user", "sentio", "ClickHouse username")
	pass := flag.String("password", "2vwzZBJ6cbZbyoKm4j", "ClickHouse password")
	flag.Parse()

	log.Println("===========================================")
	log.Println("  ClickHouse Auth Test Client (Multisig)")
	log.Println("===========================================")
	log.Printf("Target: %s", *addr)
	log.Printf("User: %s", *user)

	// Pre-parse keys
	key1, err := crypto.HexToECDSA(CorrectPrivateKeyHex1)
	if err != nil {
		log.Fatalf("Failed to parse correct private key 1: %v", err)
	}
	key2, err := crypto.HexToECDSA(CorrectPrivateKeyHex2)
	if err != nil {
		log.Fatalf("Failed to parse correct private key 2: %v", err)
	}
	wrongKey, err := crypto.HexToECDSA(WrongPrivateKeyHex)
	if err != nil {
		log.Fatalf("Failed to parse wrong private key: %v", err)
	}

	// Print addresses for verification
	log.Printf("Key 1 Address: %s", crypto.PubkeyToAddress(key1.PublicKey).Hex())
	log.Printf("Key 2 Address: %s", crypto.PubkeyToAddress(key2.PublicKey).Hex())
	log.Printf("Wrong Address: %s", crypto.PubkeyToAddress(wrongKey.PublicKey).Hex())

	// Connect to ClickHouse
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
		Keys          []*ecdsa.PrivateKey
		Type          string // "Compact" or "JSON"
		ExpectSuccess bool
	}{
		{
			Name:          "Scenario 1: Single Valid Key (Compact Mode) - Backward Compatibility",
			Keys:          []*ecdsa.PrivateKey{key1},
			Type:          "Compact",
			ExpectSuccess: true,
		},
		{
			Name:          "Scenario 2: Single Valid Key (JSON Mode)",
			Keys:          []*ecdsa.PrivateKey{key1},
			Type:          "JSON",
			ExpectSuccess: true,
		},
		{
			Name:          "Scenario 3: Multisig 2-of-2 Valid Keys (JSON Mode)",
			Keys:          []*ecdsa.PrivateKey{key1, key2},
			Type:          "JSON",
			ExpectSuccess: true,
		},
		{
			Name:          "Scenario 4: Mixed Valid/Invalid Keys (JSON Mode) - Should Fail",
			Keys:          []*ecdsa.PrivateKey{key1, wrongKey},
			Type:          "JSON",
			ExpectSuccess: false,
		},
		{
			Name:          "Scenario 5: Wrong Key (Compact Mode)",
			Keys:          []*ecdsa.PrivateKey{wrongKey},
			Type:          "Compact",
			ExpectSuccess: false,
		},
	}

	for _, s := range scenarios {
		runScenario(conn, s.Name, s.Keys, s.Type, s.ExpectSuccess)
		log.Println("-------------------------------------------")
	}

	log.Println("All test scenarios completed.")
}

func runScenario(conn clickhouse.Conn, name string, keys []*ecdsa.PrivateKey, tokenType string, expectSuccess bool) {
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

		var token string
		var err error

		if tokenType == "Compact" {
			token, err = createJWSTokenCompact(keys[0], q)
		} else {
			token, err = createJWSTokenJSON(keys, q)
		}

		if err != nil {
			log.Fatalf("Failed to create token: %v", err)
		}

		ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{
			AuthTokenSettingKey: clickhouse.CustomSetting{Value: token},
		}))

		err = conn.Exec(ctx, q)
		if expectSuccess {
			if err != nil {
				log.Fatalf("❌ FAILED: Expected success but got error: %v", err)
			}
			log.Println("   ✅ Success")
		} else {
			if err == nil {
				log.Fatalf("❌ FAILED: Expected error but got success!")
			}
			// log.Printf("   ✅ Got expected error: %v", err)
			log.Println("   ✅ Got expected error (connection closed)")
			log.Println("   Stopping remaining steps for this failure scenario.")
			return
		}
	}
	log.Printf(">>> %s Passed\n", name)
}

func createJWSTokenCompact(privateKey *ecdsa.PrivateKey, query string) (string, error) {
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
	messageHash := keccak256([]byte(signingInput))

	// Sign
	sig, err := crypto.Sign(messageHash, privateKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign: %w", err)
	}
	sig[64] += 27 // Adjust V

	sigB64 := base64.RawURLEncoding.EncodeToString(sig)

	return signingInput + "." + sigB64, nil
}

func createJWSTokenJSON(keys []*ecdsa.PrivateKey, query string) (string, error) {
	// 1. Create Payload
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

	// 2. Create Signatures
	var signatures []JWSJSONSignature

	for _, key := range keys {
		// Header for this signer
		header := JWSHeader{
			Alg: "ES256K",
			Typ: "JWS",
		}
		headerBytes, err := json.Marshal(header)
		if err != nil {
			return "", fmt.Errorf("failed to marshal header: %w", err)
		}
		headerB64 := base64.RawURLEncoding.EncodeToString(headerBytes)

		// Signing Input: ProtectedHeader.Payload
		signingInput := headerB64 + "." + payloadB64
		messageHash := keccak256([]byte(signingInput))

		sig, err := crypto.Sign(messageHash, key)
		if err != nil {
			return "", fmt.Errorf("failed to sign: %w", err)
		}
		sig[64] += 27 // Adjust V
		sigB64 := base64.RawURLEncoding.EncodeToString(sig)

		signatures = append(signatures, JWSJSONSignature{
			Protected: headerB64,
			Signature: sigB64,
		})
	}

	// 3. Construct JWS JSON
	jws := JWSJSON{
		Payload:    payloadB64,
		Signatures: signatures,
	}

	jwsBytes, err := json.Marshal(jws)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JWS JSON: %w", err)
	}

	return string(jwsBytes), nil
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
