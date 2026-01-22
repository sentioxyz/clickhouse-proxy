package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"golang.org/x/crypto/sha3"

	log "sentioxyz/sentio-core/common/log"
)

// QueryMeta describes a ClickHouse Query that is proxied to upstream.
type QueryMeta struct {
	ConnID       int64
	ClientAddr   string
	UpstreamAddr string

	// QueryPreview is a brief summary for logging/debugging.
	QueryPreview string

	// Raw can carry raw packet fragments, used as needed.
	Raw []byte

	// SQL is the Query.Body parsed precisely according to ClickHouse native protocol.
	// If parsing fails, it will be an empty string.
	SQL string

	// Settings contains query settings extracted from the ClickHouse protocol.
	Settings map[string]string
}

type Validator interface {
	ValidateQuery(context.Context, QueryMeta) error
}

// NoopValidator is the default validation implementation: always allows queries and prints the parsed SQL.
type NoopValidator struct{}

func (NoopValidator) ValidateQuery(_ context.Context, meta QueryMeta) error {
	if meta.SQL != "" {
		log.Infof("[validator] allow query from %s -> %s: %s", meta.ClientAddr, meta.UpstreamAddr, meta.SQL)
	} else if meta.QueryPreview != "" {
		log.Infof("[validator] allow query (preview) from %s -> %s: %s", meta.ClientAddr, meta.UpstreamAddr, meta.QueryPreview)
	}
	return nil
}

// AuthTokenSettingKey is the setting key used to pass the JWS authentication token.
const AuthTokenSettingKey = "x_auth_token"

// JWSHeader represents the header of a JWS token.
type JWSHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// JWSPayload represents the payload of a JWS authentication token.
type JWSPayload struct {
	// Iat is the issued-at timestamp (Unix seconds).
	Iat int64 `json:"iat"`
	// QueryHash is the Keccak256 hash of the SQL query body (hex encoded with 0x prefix).
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

// EthValidator validates queries using Ethereum-style secp256k1 signatures.
type EthValidator struct {
	// AllowedAddresses is a set of allowed Ethereum addresses (lowercase, with 0x prefix).
	AllowedAddresses map[string]bool
	// MaxTokenAge is the maximum allowed age of the token (based on iat claim).
	MaxTokenAge time.Duration
	// Enabled controls whether authentication is required; if false, all queries pass.
	Enabled bool
	// AllowNoAuth when true allows requests without auth tokens to pass through.
	AllowNoAuth bool
}

// NewEthValidator creates a new EthValidator with the given allowed addresses.
func NewEthValidator(addresses []string, maxAge time.Duration, enabled bool, allowNoAuth bool) *EthValidator {
	allowed := make(map[string]bool, len(addresses))
	for _, addr := range addresses {
		allowed[strings.ToLower(addr)] = true
	}
	return &EthValidator{
		AllowedAddresses: allowed,
		MaxTokenAge:      maxAge,
		Enabled:          enabled,
		AllowNoAuth:      allowNoAuth,
	}
}

// ValidateQuery validates the query using the x_auth_token setting.
func (v *EthValidator) ValidateQuery(ctx context.Context, meta QueryMeta) error {
	if !v.Enabled {
		return nil
	}

	token, ok := meta.Settings[AuthTokenSettingKey]
	if !ok || token == "" {
		if v.AllowNoAuth {
			log.Infof("[eth_validator] no auth token, allowing due to allow_no_auth=true")
			return nil
		}
		return errors.New("missing authentication token")
	}

	// Trim possible quotes that might be added by some client libs
	token = strings.Trim(token, "\"'")

	// Determine if it's JSON or Compact serialization
	if strings.HasPrefix(strings.TrimSpace(token), "{") {
		return v.validateJWSJSON(token, meta.SQL)
	}
	return v.validateJWSCompact(token, meta.SQL)
}

func (v *EthValidator) validateJWSCompact(token, sql string) error {
	header, payload, signature, err := parseJWSCompact(token)
	if err != nil {
		return fmt.Errorf("invalid JWS token: %w", err)
	}

	if err := v.verifyPayloadAndHeader(header, payload, sql); err != nil {
		return err
	}

	// Verify signature and recover address
	signingInput := token[:strings.LastIndex(token, ".")]
	recoveredAddr, err := v.recoverAddressFromInput(signingInput, signature)
	if err != nil {
		return fmt.Errorf("signature verification failed: %w", err)
	}

	// Check allowlist
	if !v.AllowedAddresses[strings.ToLower(recoveredAddr)] {
		return fmt.Errorf("address %s not in allowlist", recoveredAddr)
	}

	log.Infof("[eth_validator] authenticated query (compact) from %s (address: %s)", "", recoveredAddr)
	return nil
}

func (v *EthValidator) validateJWSJSON(token, sql string) error {
	var jws JWSJSON
	if err := json.Unmarshal([]byte(token), &jws); err != nil {
		return fmt.Errorf("invalid JWS JSON: %w", err)
	}

	if len(jws.Signatures) == 0 {
		return errors.New("no signatures found in JWS JSON")
	}

	// All signatures must be valid and from allowed addresses
	var authenticatedAddresses []string

	for i, sig := range jws.Signatures {
		headerBytes, err := base64.RawURLEncoding.DecodeString(sig.Protected)
		if err != nil {
			return fmt.Errorf("sig[%d]: invalid protected header encoding: %w", i, err)
		}
		var header JWSHeader
		if err := json.Unmarshal(headerBytes, &header); err != nil {
			return fmt.Errorf("sig[%d]: invalid header JSON: %w", i, err)
		}

		// Decode payload to verify hash/time (assuming same payload for all, which is how JWS works)
		payloadBytes, err := base64.RawURLEncoding.DecodeString(jws.Payload)
		if err != nil {
			return fmt.Errorf("invalid payload encoding: %w", err)
		}
		var payload JWSPayload
		if err := json.Unmarshal(payloadBytes, &payload); err != nil {
			return fmt.Errorf("invalid payload JSON: %w", err)
		}

		// Verify header and payload constraints
		if err := v.verifyPayloadAndHeader(header, payload, sql); err != nil {
			return fmt.Errorf("sig[%d]: %w", i, err)
		}

		// Verify signature
		signatureBytes, err := base64.RawURLEncoding.DecodeString(sig.Signature)
		if err != nil {
			return fmt.Errorf("sig[%d]: invalid signature encoding: %w", i, err)
		}

		signingInput := sig.Protected + "." + jws.Payload
		recoveredAddr, err := v.recoverAddressFromInput(signingInput, signatureBytes)
		if err != nil {
			return fmt.Errorf("sig[%d]: signature verification failed: %w", i, err)
		}

		if !v.AllowedAddresses[strings.ToLower(recoveredAddr)] {
			return fmt.Errorf("sig[%d]: address %s not in allowlist", i, recoveredAddr)
		}
		authenticatedAddresses = append(authenticatedAddresses, recoveredAddr)
	}

	log.Infof("[eth_validator] authenticated query (json) from %s (addresses: %v)", "", authenticatedAddresses)
	return nil
}

func (v *EthValidator) verifyPayloadAndHeader(header JWSHeader, payload JWSPayload, sql string) error {
	// Verify algorithm
	if header.Alg != "ES256K" && header.Alg != "secp256k1" {
		return fmt.Errorf("unsupported algorithm: %s", header.Alg)
	}

	// Verify timestamp
	now := time.Now().Unix()
	tokenAge := now - payload.Iat
	if tokenAge < 0 {
		return errors.New("token issued in the future")
	}
	if time.Duration(tokenAge)*time.Second > v.MaxTokenAge {
		return fmt.Errorf("token expired: age %ds exceeds max %s", tokenAge, v.MaxTokenAge)
	}

	// Verify query hash
	expectedHash := keccak256Hex([]byte(sql))
	if !strings.EqualFold(payload.QueryHash, expectedHash) {
		return fmt.Errorf("query hash mismatch: expected %s, got %s", expectedHash, payload.QueryHash)
	}
	return nil
}

func (v *EthValidator) recoverAddressFromInput(signingInput string, signature []byte) (string, error) {
	messageHash := keccak256([]byte(signingInput))
	return recoverAddress(messageHash, signature)
}

// parseJWSCompact parses a JWS compact serialization token into its components.
func parseJWSCompact(token string) (JWSHeader, JWSPayload, []byte, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return JWSHeader{}, JWSPayload{}, nil, errors.New("invalid JWS format: expected 3 parts")
	}

	// Decode header
	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return JWSHeader{}, JWSPayload{}, nil, fmt.Errorf("invalid header encoding: %w", err)
	}
	var header JWSHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return JWSHeader{}, JWSPayload{}, nil, fmt.Errorf("invalid header JSON: %w", err)
	}

	// Decode payload
	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return JWSHeader{}, JWSPayload{}, nil, fmt.Errorf("invalid payload encoding: %w", err)
	}
	var payload JWSPayload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return JWSHeader{}, JWSPayload{}, nil, fmt.Errorf("invalid payload JSON: %w", err)
	}

	// Decode signature
	signature, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return JWSHeader{}, JWSPayload{}, nil, fmt.Errorf("invalid signature encoding: %w", err)
	}

	return header, payload, signature, nil
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

// recoverAddress recovers the Ethereum address from a message hash and signature.
// The signature must be 65 bytes (R || S || V) where V is 0 or 1 (not 27/28).
func recoverAddress(messageHash, signature []byte) (string, error) {
	if len(signature) != 65 {
		return "", fmt.Errorf("invalid signature length: %d (expected 65)", len(signature))
	}

	// Adjust V if it's 27 or 28 (Ethereum convention)
	sig := make([]byte, 65)
	copy(sig, signature)
	if sig[64] >= 27 {
		sig[64] -= 27
	}

	// Use go-ethereum's crypto package to recover the public key
	pubKey, err := ecrecover(messageHash, sig)
	if err != nil {
		return "", err
	}

	// Compute address from public key (Keccak256 of pubkey[1:], take last 20 bytes)
	addr := keccak256(pubKey[1:])
	return "0x" + hex.EncodeToString(addr[12:]), nil
}

// ecrecover recovers the uncompressed public key from a message hash and signature.
func ecrecover(hash, sig []byte) ([]byte, error) {
	return crypto.Ecrecover(hash, sig)
}
