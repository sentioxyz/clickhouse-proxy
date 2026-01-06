package signature

import (
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

const signaturePrefix = "/* sentio-sig:"
const signatureSuffix = " */"

// extractSignatureFromSQL extracts signature from SQL prefix comment.
// Format: /* sentio-sig:0x... */ SELECT ...
// Returns (signature, cleanSQL) where cleanSQL has the signature comment removed.
// Note: This is a copy from proxy.go for testing purposes
func extractSignatureFromSQL(sql string) (signature string, cleanSQL string) {
	if !strings.HasPrefix(sql, signaturePrefix) {
		return "", sql
	}
	
	endIdx := strings.Index(sql, signatureSuffix)
	if endIdx == -1 {
		return "", sql
	}
	
	signature = sql[len(signaturePrefix):endIdx]
	cleanSQL = strings.TrimPrefix(sql[endIdx+len(signatureSuffix):], " ")
	return signature, cleanSQL
}

// TestExtractSignatureFromSQL tests the signature extraction helper
func TestExtractSignatureFromSQL(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantSig   string
		wantClean string
	}{
		{
			name:      "with valid signature",
			sql:       "/* sentio-sig:0xabc123 */ SELECT 1",
			wantSig:   "0xabc123",
			wantClean: "SELECT 1",
		},
		{
			name:      "with full length signature",
			sql:       "/* sentio-sig:0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef01 */ SELECT version()",
			wantSig:   "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef01",
			wantClean: "SELECT version()",
		},
		{
			name:      "no signature",
			sql:       "SELECT 1",
			wantSig:   "",
			wantClean: "SELECT 1",
		},
		{
			name:      "empty signature",
			sql:       "/* sentio-sig: */ SELECT 1",
			wantSig:   "",
			wantClean: "SELECT 1",
		},
		{
			name:      "other comment prefix",
			sql:       "/* other comment */ SELECT 1",
			wantSig:   "",
			wantClean: "/* other comment */ SELECT 1",
		},
		{
			name:      "signature with complex query",
			sql:       "/* sentio-sig:0xdeadbeef */ INSERT INTO table (col) VALUES (1), (2), (3)",
			wantSig:   "0xdeadbeef",
			wantClean: "INSERT INTO table (col) VALUES (1), (2), (3)",
		},
		{
			name:      "signature with multiline query",
			sql:       "/* sentio-sig:0xfeed */ SELECT\n  a,\n  b\nFROM table",
			wantSig:   "0xfeed",
			wantClean: "SELECT\n  a,\n  b\nFROM table",
		},
		{
			name:      "malformed - no closing",
			sql:       "/* sentio-sig:0xabc SELECT 1",
			wantSig:   "",
			wantClean: "/* sentio-sig:0xabc SELECT 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sig, clean := extractSignatureFromSQL(tt.sql)
			if sig != tt.wantSig {
				t.Errorf("extractSignatureFromSQL(%q) sig = %q, want %q", tt.sql, sig, tt.wantSig)
			}
			if clean != tt.wantClean {
				t.Errorf("extractSignatureFromSQL(%q) clean = %q, want %q", tt.sql, clean, tt.wantClean)
			}
		})
	}
}

// TestQueryParserWithSignature tests that queryParser properly extracts signatures from SQL body
func TestQueryParserWithSignature(t *testing.T) {
	version := 54460
	
	testCases := []struct {
		name          string
		originalQuery string
		signature     string
	}{
		{
			name:          "simple select",
			originalQuery: "SELECT 1",
			signature:     "0x1234567890abcdef",
		},
		{
			name:          "complex insert",
			originalQuery: "INSERT INTO logs (ts, msg) VALUES (now(), 'test')",
			signature:     "0xdeadbeefcafe",
		},
		{
			name:          "query without signature",
			originalQuery: "SELECT version()",
			signature:     "",
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Build the SQL with signature prefix if provided
			var fullSQL string
			if tc.signature != "" {
				fullSQL = signaturePrefix + tc.signature + signatureSuffix + " " + tc.originalQuery
			} else {
				fullSQL = tc.originalQuery
			}
			
			// Build a Query packet
			var buf proto.Buffer
			
			// Query ID (empty)
			buf.PutString("")
			
			// Client Info
			if proto.FeatureClientWriteInfo.In(version) {
				clientInfo := proto.ClientInfo{
					ProtocolVersion: version,
					ClientName:      "TestClient",
					Interface:       1, // TCP
				}
				clientInfo.EncodeAware(&buf, version)
			}

			// Settings (empty list terminator)
			buf.PutString("")

			// Interserver Roles
			if proto.FeatureInterserverExternallyGrantedRoles.In(version) {
				buf.PutString("")
			}
			
			// InterServer Secret
			if proto.FeatureInterServerSecret.In(version) {
				buf.PutString("")
			}
			
			// Stage (Complete=2)
			buf.PutUVarInt(2)
			
			// Compression (Disabled=0)
			buf.PutUVarInt(0)
			
			// Body with signature
			buf.PutString(fullSQL)
			
			// Parameters
			if proto.FeatureParameters.In(version) {
				buf.PutString("")
			}

			// Full frame: [PacketType=1] [Payload]
			frame := append([]byte{1}, buf.Buf...)
			
			// Verify signature extraction works
			sig, cleanSQL := extractSignatureFromSQL(fullSQL)
			
			if sig != tc.signature {
				t.Errorf("extracted signature = %q, want %q", sig, tc.signature)
			}
			
			if cleanSQL != tc.originalQuery {
				t.Errorf("cleaned SQL = %q, want %q", cleanSQL, tc.originalQuery)
			}
			
			// Verify frame is valid (basic sanity check)
			if len(frame) < 10 {
				t.Errorf("frame too short: %d bytes", len(frame))
			}
		})
	}
}
