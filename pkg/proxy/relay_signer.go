package proxy

// relay_signer.go — Relay JWS token signer for proxy-to-proxy (__route__) connections.
//
// When Proxy1 processes a __route__ connection from ClickHouse, it intercepts the first
// Query packet, signs a relay JWS token for the sub-query SQL using a shared Ethereum
// private key, and injects the token into the Query's Settings before forwarding to Proxy2.
//
// Proxy2 then validates this relay token through its normal JWS validation flow.

import (
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

// RelaySigner signs JWS tokens for proxy-to-proxy relay connections.
// All proxies in a cluster share the same private key.
type RelaySigner struct {
	privateKey *ecdsa.PrivateKey
	address    string // Ethereum address (lowercase, with 0x prefix)
}

// NewRelaySigner creates a new RelaySigner from a hex-encoded Ethereum private key.
func NewRelaySigner(privateKeyHex string) (*RelaySigner, error) {
	privateKeyHex = strings.TrimPrefix(privateKeyHex, "0x")
	privKey, err := crypto.HexToECDSA(privateKeyHex)
	if err != nil {
		return nil, fmt.Errorf("invalid relay private key: %w", err)
	}
	addr := strings.ToLower(crypto.PubkeyToAddress(privKey.PublicKey).Hex())
	return &RelaySigner{
		privateKey: privKey,
		address:    addr,
	}, nil
}

// Address returns the Ethereum address corresponding to the relay private key.
func (s *RelaySigner) Address() string {
	return s.address
}

// SignToken creates a JWS compact serialization token for the given SQL query.
// The token contains:
//   - Header: {"alg":"ES256K","typ":"JWT"}
//   - Payload: {"iat":<now>, "qhash":<keccak256(sql)>}
//   - Signature: secp256k1 signature over keccak256(header.payload)
func (s *RelaySigner) SignToken(sql string) (string, error) {
	// Build header
	header := JWSHeader{Alg: "ES256K", Typ: "JWT"}
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return "", fmt.Errorf("marshal header: %w", err)
	}
	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)

	// Build payload
	payload := JWSPayload{
		Iat:       time.Now().Unix(),
		QueryHash: keccak256Hex([]byte(sql)),
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	payloadB64 := base64.RawURLEncoding.EncodeToString(payloadJSON)

	// Signing input
	signingInput := headerB64 + "." + payloadB64

	// Sign with Ethereum style (Keccak256 of signing input)
	hash := keccak256([]byte(signingInput))
	sig, err := crypto.Sign(hash, s.privateKey)
	if err != nil {
		return "", fmt.Errorf("sign: %w", err)
	}

	// Adjust V (0/1 -> 27/28) for Ethereum convention
	sig[64] += 27

	signatureB64 := base64.RawURLEncoding.EncodeToString(sig)
	return signingInput + "." + signatureB64, nil
}
