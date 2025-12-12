package main

import (
	"context"
	"log"
)

// QueryMeta 描述一条被代理到上游的 ClickHouse Query。
type QueryMeta struct {
	ConnID       int64
	ClientAddr   string
	UpstreamAddr string

	// QueryPreview 是一个用于日志/调试的简短摘要。
	QueryPreview string

	// Raw 可以携带原始包片段，视需要使用。
	Raw []byte

	// SQL 是按照 ClickHouse 原生协议精确解析出来的 Query.Body。
	// 如果解析失败，则为空字符串。
	SQL string
}

type Validator interface {
	ValidateQuery(context.Context, QueryMeta) error
}

// NoopValidator 是默认的验证实现：永远放行，并打印解析出的 SQL。
type NoopValidator struct{}

func (NoopValidator) ValidateQuery(_ context.Context, meta QueryMeta) error {
	if meta.SQL != "" {
		log.Printf("[validator] allow query from %s -> %s: %s", meta.ClientAddr, meta.UpstreamAddr, meta.SQL)
	} else if meta.QueryPreview != "" {
		log.Printf("[validator] allow query (preview) from %s -> %s: %s", meta.ClientAddr, meta.UpstreamAddr, meta.QueryPreview)
	}
	return nil
}
