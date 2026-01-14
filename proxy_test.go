package main

import (
	"encoding/binary"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	drvproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
)

func TestDetectPacketType(t *testing.T) {
	tests := []struct {
		name string
		buf  []byte
		want string
	}{
		{name: "query", buf: []byte{1}, want: "Query"},
		{name: "data", buf: []byte{2}, want: "Data"},
		{name: "small unknown", buf: []byte{10}, want: "type_10"},
		{name: "unknown when continuation bit set", buf: []byte{0x81, 0x01}, want: "unknown"},
		{name: "empty", buf: []byte{}, want: "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detectPacketType(tt.buf)
			if got != tt.want {
				t.Fatalf("detectPacketType(%v) = %q, want %q", tt.buf, got, tt.want)
			}
		})
	}

	// Verify multi-byte varint still resolves.
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], 3) // Cancel
	if got := detectPacketType(buf[:n]); got != "Cancel" {
		t.Fatalf("detectPacketType(varint Cancel) = %q, want Cancel", got)
	}
}

func TestSummarizePrintable(t *testing.T) {
	in := []byte("SELECT\x00 \t\n1")
	got := summarizePrintable(in, 50)
	want := "SELECT 1"
	if got != want {
		t.Fatalf("summarizePrintable = %q, want %q", got, want)
	}

	// Respect maxLen truncation.
	long := []byte("abcdefghijklmnopqrstuvwxyz")
	if got := summarizePrintable(long, 5); got != "abcde" {
		t.Fatalf("summarizePrintable truncation = %q, want %q", got, "abcde")
	}
}

func TestExtractQuerySummary(t *testing.T) {
	raw := []byte("prefix stuff SELECT sum(id) FROM t")
	got := extractQuerySummary(raw, 100)
	want := "SELECT sum(id) FROM t"
	if got != want {
		t.Fatalf("extractQuerySummary = %q, want %q", got, want)
	}
}

// Test that our queryParser can decode a Query packet produced by
// clickhouse-go/v2's proto.Query.Encode.
func TestQueryParser_DriverQuerySimple(t *testing.T) {
	var buf chproto.Buffer

	q := drvproto.Query{
		ID:                       "test-id",
		ClientName:               "test-client",
		ClientVersion:            drvproto.Version{Major: 1, Minor: 0, Patch: 0},
		ClientTCPProtocolVersion: drvproto.DBMS_TCP_PROTOCOL_VERSION,
		Body:                     "SELECT 1",
		Compression:              false,
	}

	if err := q.Encode(&buf, drvproto.DBMS_TCP_PROTOCOL_VERSION); err != nil {
		t.Fatalf("encode driver query: %v", err)
	}

	frame := append([]byte{drvproto.ClientQuery}, buf.Buf...)

	p := &queryParser{version: int(drvproto.DBMS_TCP_PROTOCOL_VERSION), addendumDone: true}
	sqls, err := p.feed(frame)
	if err != nil {
		t.Fatalf("queryParser.feed error: %v", err)
	}
	if len(sqls) != 1 {
		t.Fatalf("expected 1 SQL, got %d", len(sqls))
	}
	if sqls[0].Body != q.Body {
		t.Fatalf("decoded body = %q, want %q", sqls[0].Body, q.Body)
	}
}

func TestQueryParser_DriverQuerySplit(t *testing.T) {
	var buf chproto.Buffer

	q := drvproto.Query{
		ID:                       "split-id",
		ClientName:               "test-client",
		ClientVersion:            drvproto.Version{Major: 1, Minor: 0, Patch: 0},
		ClientTCPProtocolVersion: drvproto.DBMS_TCP_PROTOCOL_VERSION,
		Body:                     "SELECT 42",
		Compression:              false,
	}
	if err := q.Encode(&buf, drvproto.DBMS_TCP_PROTOCOL_VERSION); err != nil {
		t.Fatalf("encode driver query: %v", err)
	}
	frame := append([]byte{drvproto.ClientQuery}, buf.Buf...)

	for split := 1; split < len(frame)-1; split++ {
		p := &queryParser{version: int(drvproto.DBMS_TCP_PROTOCOL_VERSION), addendumDone: true}
		first := frame[:split]
		second := frame[split:]

		if sqls, err := p.feed(first); err != nil {
			t.Fatalf("split=%d first feed error: %v", split, err)
		} else if len(sqls) != 0 {
			t.Fatalf("split=%d expected 0 SQLs after first feed, got %d", split, len(sqls))
		}

		sqls, err := p.feed(second)
		if err != nil {
			t.Fatalf("split=%d second feed error: %v", split, err)
		}
		if len(sqls) != 1 {
			t.Fatalf("split=%d expected 1 SQL after second feed, got %d", split, len(sqls))
		}
		if sqls[0].Body != q.Body {
			t.Fatalf("split=%d body=%q, want %q", split, sqls[0].Body, q.Body)
		}
	}
}

// Simulate the case where an addendum string (e.g. quota key) and the Query
// packet are coalesced into a single TCP read. The parser must still be able
// to find and decode the Query.
func TestQueryParser_AddendumAndQuerySameChunk(t *testing.T) {
	var add chproto.Buffer
	add.PutString("") // empty quota key, matches sendAddendum

	var qbuf chproto.Buffer
	q := drvproto.Query{
		ID:                       "addendum-id",
		ClientName:               "test-client",
		ClientVersion:            drvproto.Version{Major: 1, Minor: 0, Patch: 0},
		ClientTCPProtocolVersion: drvproto.DBMS_TCP_PROTOCOL_VERSION,
		Body:                     "SELECT 100",
		Compression:              false,
	}
	if err := q.Encode(&qbuf, drvproto.DBMS_TCP_PROTOCOL_VERSION); err != nil {
		t.Fatalf("encode driver query: %v", err)
	}

	frame := append([]byte{drvproto.ClientQuery}, qbuf.Buf...)
	payload := append(add.Buf, frame...)

	p := &queryParser{version: int(drvproto.DBMS_TCP_PROTOCOL_VERSION)}
	sqls, err := p.feed(payload)
	if err != nil {
		t.Fatalf("feed error: %v", err)
	}
	if len(sqls) != 1 {
		t.Fatalf("expected 1 SQL, got %d", len(sqls))
	}
	if sqls[0].Body != q.Body {
		t.Fatalf("body=%q, want %q", sqls[0].Body, q.Body)
	}
}

func TestExtractSignatureFromSQL(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		wantSignature string
		wantCleanSQL  string
	}{
		{
			name:          "basic",
			sql:           "/* jwk_signature=eyJxhGcc */ SELECT 1",
			wantSignature: "eyJxhGcc",
			wantCleanSQL:  "SELECT 1",
		},
		{
			name:          "with spaces",
			sql:           "/*   jwk_signature=abc.def.ghi   */ INSERT INTO table (a) VALUES (1)",
			wantSignature: "abc.def.ghi",
			wantCleanSQL:  "INSERT INTO table (a) VALUES (1)",
		},
		{
			name:          "clean sql trailing spaces trim",
			sql:           "/* jwk_signature=sig */   SELECT 1",
			wantSignature: "sig",
			wantCleanSQL:  "SELECT 1",
		},
		{
			name:          "no signature",
			sql:           "SELECT 1",
			wantSignature: "",
			wantCleanSQL:  "SELECT 1",
		},
		{
			name:          "wrong prefix",
			sql:           "/* signatures=abc */ SELECT 1",
			wantSignature: "",
			wantCleanSQL:  "/* signatures=abc */ SELECT 1",
		},
		{
			name:          "multiline comment",
			sql:           "/*\n jwk_signature=xyz \n*/ SELECT 1",
			wantSignature: "xyz",
			wantCleanSQL:  "SELECT 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSig, gotClean := extractSignatureFromSQL(tt.sql)
			if gotSig != tt.wantSignature {
				t.Errorf("extractSignatureFromSQL() signature = %q, want %q", gotSig, tt.wantSignature)
			}
			if gotClean != tt.wantCleanSQL {
				t.Errorf("extractSignatureFromSQL() cleanSQL = %q, want %q", gotClean, tt.wantCleanSQL)
			}
		})
	}
}
