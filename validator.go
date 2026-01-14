package main

import (
	"context"
	"errors"
	"fmt"

	log "sentioxyz/sentio-core/common/log"

	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
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

	// Signature matches the JWK signature passed in the SQL comment.
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

// JWKValidator enforces that queries are signed by a valid JWT.
type JWKValidator struct {
	KeySet jwk.Set
}

func NewJWKValidator(keySet jwk.Set) *JWKValidator {
	return &JWKValidator{KeySet: keySet}
}

func (v *JWKValidator) ValidateQuery(_ context.Context, meta QueryMeta) error {
	// If no signature provided, reject.
	if meta.Signature == "" {
		return errors.New("missing signature in SQL comment")
	}

	// Parse and verify JWT
	// jwt.Parse checks signature and valid claims (exp, nbf) by default.
	token, err := jwt.Parse([]byte(meta.Signature), jwt.WithKeySet(v.KeySet), jwt.WithValidate(true))
	if err != nil {
		return fmt.Errorf("invalid jwk signature: %w", err)
	}

	// Log successful authentication
	log.Infof("[validator] authenticated query from %s (iss: %s, sub: %s)", meta.ClientAddr, token.Issuer(), token.Subject())
	return nil
}
