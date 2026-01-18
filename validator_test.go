package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

// generateTestToken creates a JWS token for testing using Ethereum-style signing.
func generateTestToken(t *testing.T, privKeyHex string, iat time.Time, qhash string) string {
	t.Helper()

	privateKey, err := crypto.HexToECDSA(privKeyHex)
	if err != nil {
		t.Fatalf("failed to parse private key: %v", err)
	}

	// Build JWS header
	header := JWSHeader{Alg: "ES256K", Typ: "JWT"}
	headerJSON, _ := json.Marshal(header)
	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)

	// Build JWS payload
	payload := JWSPayload{Iat: iat.Unix(), QueryHash: qhash}
	payloadJSON, _ := json.Marshal(payload)
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	// Signing input
	signingInput := headerB64 + "." + payloadB64

	// Sign with Ethereum style (Keccak256 of signing input)
	hash := keccak256([]byte(signingInput))
	sig, err := crypto.Sign(hash, privateKey)
	if err != nil {
		t.Fatalf("failed to sign: %v", err)
	}

	// Adjust V (0/1 -> 27/28) for Ethereum convention
	sig[64] += 27

	signatureB64 := base64.RawURLEncoding.EncodeToString(sig)
	return signingInput + "." + signatureB64
}

func TestEthValidator_ValidToken(t *testing.T) {
	// Use a known test private key
	privKeyHex := "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"

	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true)

	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))

	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL: sql,
		Settings: map[string]string{
			AuthTokenSettingKey: token,
		},
	}

	err := validator.ValidateQuery(context.Background(), meta)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestEthValidator_MissingToken(t *testing.T) {
	privKeyHex := "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true)

	meta := QueryMeta{
		SQL:      "SELECT 1",
		Settings: map[string]string{},
	}

	err := validator.ValidateQuery(context.Background(), meta)
	if err == nil {
		t.Error("expected error for missing token, got nil")
	}
}

func TestEthValidator_ExpiredToken(t *testing.T) {
	privKeyHex := "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true)

	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))

	// Token issued 2 minutes ago (expired)
	token := generateTestToken(t, privKeyHex, time.Now().Add(-2*time.Minute), sqlHash)

	meta := QueryMeta{
		SQL: sql,
		Settings: map[string]string{
			AuthTokenSettingKey: token,
		},
	}

	err := validator.ValidateQuery(context.Background(), meta)
	if err == nil {
		t.Error("expected error for expired token, got nil")
	}
}

func TestEthValidator_QueryHashMismatch(t *testing.T) {
	privKeyHex := "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
	privKey, _ := crypto.HexToECDSA(privKeyHex)
	addr := crypto.PubkeyToAddress(privKey.PublicKey).Hex()

	validator := NewEthValidator([]string{addr}, 1*time.Minute, true)

	sql := "SELECT 1"
	// Wrong hash - hash of different query
	wrongHash := keccak256Hex([]byte("SELECT 2"))

	token := generateTestToken(t, privKeyHex, time.Now(), wrongHash)

	meta := QueryMeta{
		SQL: sql,
		Settings: map[string]string{
			AuthTokenSettingKey: token,
		},
	}

	err := validator.ValidateQuery(context.Background(), meta)
	if err == nil {
		t.Error("expected error for query hash mismatch, got nil")
	}
}

func TestEthValidator_UnauthorizedAddress(t *testing.T) {
	// Validator allows a different address
	validator := NewEthValidator([]string{"0x1234567890123456789012345678901234567890"}, 1*time.Minute, true)

	// Use our test key which has a different address
	privKeyHex := "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"

	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))

	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	meta := QueryMeta{
		SQL: sql,
		Settings: map[string]string{
			AuthTokenSettingKey: token,
		},
	}

	err := validator.ValidateQuery(context.Background(), meta)
	if err == nil {
		t.Error("expected error for unauthorized address, got nil")
	}
}

func TestEthValidator_Disabled(t *testing.T) {
	// Validator is disabled
	validator := NewEthValidator(nil, 1*time.Minute, false)

	meta := QueryMeta{
		SQL:      "SELECT 1",
		Settings: map[string]string{},
	}

	err := validator.ValidateQuery(context.Background(), meta)
	if err != nil {
		t.Errorf("expected no error when validator is disabled, got: %v", err)
	}
}

func TestParseJWS(t *testing.T) {
	privKeyHex := "4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318"
	sql := "SELECT 1"
	sqlHash := keccak256Hex([]byte(sql))

	token := generateTestToken(t, privKeyHex, time.Now(), sqlHash)

	header, payload, sig, err := parseJWSCompact(token)
	if err != nil {
		t.Fatalf("parseJWSCompact failed: %v", err)
	}

	if header.Alg != "ES256K" {
		t.Errorf("expected alg ES256K, got %s", header.Alg)
	}
	if payload.QueryHash != sqlHash {
		t.Errorf("expected qhash %s, got %s", sqlHash, payload.QueryHash)
	}
	if len(sig) != 65 {
		t.Errorf("expected signature length 65, got %d", len(sig))
	}
}
