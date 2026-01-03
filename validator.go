package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
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

	// Signature matches the Ethereum signature passed in the QuotaKey field.
	Signature string
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

// SignatureValidator enforces that queries are signed by an allowed Ethereum address.
type SignatureValidator struct {
	AllowedProcessors map[string]struct{}
}

func NewSignatureValidator(allowed []string) *SignatureValidator {
	m := make(map[string]struct{})
	for _, a := range allowed {
		m[strings.ToLower(a)] = struct{}{}
	}
	return &SignatureValidator{AllowedProcessors: m}
}

func (v *SignatureValidator) ValidateQuery(_ context.Context, meta QueryMeta) error {
	// If no signature provided, reject.
	if meta.Signature == "" {
		return errors.New("missing signature in QuotaKey")
	}

	// 1. Decode hex signature
	sigHex := strings.TrimPrefix(meta.Signature, "0x")
	sigBytes, err := hex.DecodeString(sigHex)
	if err != nil {
		return fmt.Errorf("invalid hex signature: %w", err)
	}

	// 2. Hash the SQL body
	hash := crypto.Keccak256Hash([]byte(meta.SQL))

	// 3. Recover Public Key
	// crypto.SigToPub expects a 65-byte signature (R, S, V)
	pubKey, err := crypto.SigToPub(hash.Bytes(), sigBytes)
	if err != nil {
		return fmt.Errorf("signature recovery failed: %w", err)
	}

	// 4. Derive Address
	addr := crypto.PubkeyToAddress(*pubKey).Hex()

	// 5. Check Allowlist
	if _, ok := v.AllowedProcessors[strings.ToLower(addr)]; !ok {
		return fmt.Errorf("unauthorized signer: %s", addr)
	}
	
	log.Infof("[validator] authenticated query from %s (signer: %s)", meta.ClientAddr, addr)
	return nil
}
