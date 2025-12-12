package main

import (
	"context"
	"log"
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
}

type Validator interface {
	ValidateQuery(context.Context, QueryMeta) error
}

// NoopValidator is the default validation implementation: always allows queries and prints the parsed SQL.
type NoopValidator struct{}

func (NoopValidator) ValidateQuery(_ context.Context, meta QueryMeta) error {
	if meta.SQL != "" {
		log.Printf("[validator] allow query from %s -> %s: %s", meta.ClientAddr, meta.UpstreamAddr, meta.SQL)
	} else if meta.QueryPreview != "" {
		log.Printf("[validator] allow query (preview) from %s -> %s: %s", meta.ClientAddr, meta.UpstreamAddr, meta.QueryPreview)
	}
	return nil
}
