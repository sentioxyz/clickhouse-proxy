package main

import (
	"encoding/binary"
	"testing"
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
