package main

import "context"

type QueryMeta struct {
	ConnID       int64
	ClientAddr   string
	UpstreamAddr string
	QueryPreview string
	Raw          []byte
}

type Validator interface {
	ValidateQuery(context.Context, QueryMeta) error
}

type NoopValidator struct{}

func (NoopValidator) ValidateQuery(context.Context, QueryMeta) error { return nil }
