package main

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

// TestQueryParser_SignatureFragmented verifies that the parser correctly reassembles
// a Query packet with a Signature (QuotaKey) even when delivered byte-by-byte.
func TestQueryParser_SignatureFragmented(t *testing.T) {
	version := 54460
	query := "SELECT 1"
	sig := "0x1234567890abcdef"

	var buf proto.Buffer
	
	// 1. Query ID (empty)
	buf.PutString("")
	
	// 2. Client Info (if enabled)
	if proto.FeatureClientWriteInfo.In(version) {
		clientInfo := proto.ClientInfo{
			ProtocolVersion: version,
			ClientName:      "Test",
			QuotaKey:        sig,
			Interface:       1, // TCP
		}
		clientInfo.EncodeAware(&buf, version)
	}

	// 3. Settings
	// FeatureSettingsSerializedAsStrings is true for 54460
	// Write empty setting to terminate loop
	// proto.Setting.Encode writes Key(String). If String is empty, loop terminates.
	buf.PutString("") 

	// 4. Interserver Roles
	if proto.FeatureInterserverExternallyGrantedRoles.In(version) {
		buf.PutString("")
	}
	
	// 5. InterServer Secret
	if proto.FeatureInterServerSecret.In(version) {
		buf.PutString("")
	}
	
	// 6. Stage (Complete=2)
	buf.PutUVarInt(2)
	
	// 7. Compression (Disabled=0)
	buf.PutUVarInt(0)
	
	// 8. Body
	buf.PutString(query)
	
	// 9. Parameters
	if proto.FeatureParameters.In(version) {
		// Empty parameter key terminates loop
		buf.PutString("")
	}

	// Full frame: [PacketType] [Payload]
	payload := buf.Buf
	frame := append([]byte{1}, payload...)

	// Feed it byte-by-byte
	p := &queryParser{version: version, addendumDone: true}
	
	var foundParsed bool
	for i := 0; i < len(frame); i++ {
		chunk := frame[i : i+1]
		sqls, err := p.feed(chunk)
		if err != nil {
			t.Fatalf("feed error at byte %d: %v", i, err)
		}
		if len(sqls) > 0 {
			if foundParsed {
				t.Fatalf("parser returned multiple queries for single packet")
			}
			foundParsed = true
			
			if len(sqls) != 1 {
				t.Fatalf("expected 1 SQL, got %d", len(sqls))
			}
			res := sqls[0]
			if res.Body != query {
				t.Errorf("expected Body %q, got %q", query, res.Body)
			}
			if res.Signature != sig {
				t.Errorf("expected Signature %q, got %q", sig, res.Signature)
			}
		}
	}
	
	if !foundParsed {
		t.Fatal("parser never returned a completed query")
	}
}
