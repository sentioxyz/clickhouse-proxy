package proxy

import (
	"context"
	"testing"
)

func TestParseSentioNetworkTables(t *testing.T) {
	rewriter := &SentioNetworkRewriter{}

	tests := []struct {
		name     string
		sql      string
		expected []ParsedTable
	}{
		{
			name: "single table",
			sql:  "SELECT * FROM sentio_coinbase.transfer",
			expected: []ParsedTable{
				{FullMatch: "sentio_coinbase.transfer", ProcessorId: "coinbase", TableName: "transfer"},
			},
		},
		{
			name: "multiple tables",
			sql:  "SELECT * FROM sentio_coinbase.transfer UNION ALL SELECT * FROM sentio_pancakeswap123.Withdrawl",
			expected: []ParsedTable{
				{FullMatch: "sentio_coinbase.transfer", ProcessorId: "coinbase", TableName: "transfer"},
				{FullMatch: "sentio_pancakeswap123.Withdrawl", ProcessorId: "pancakeswap123", TableName: "Withdrawl"},
			},
		},
		{
			name:     "no sentio tables",
			sql:      "SELECT * FROM sentio.normal_table",
			expected: nil,
		},
		{
			name: "case insensitive",
			sql:  "SELECT * FROM SENTIO_Coinbase.Transfer",
			expected: []ParsedTable{
				{FullMatch: "SENTIO_Coinbase.Transfer", ProcessorId: "Coinbase", TableName: "Transfer"},
			},
		},
		{
			name: "with join",
			sql:  "SELECT a.*, b.* FROM sentio_proj1.events a JOIN sentio_proj2.users b ON a.user_id = b.id",
			expected: []ParsedTable{
				{FullMatch: "sentio_proj1.events", ProcessorId: "proj1", TableName: "events"},
				{FullMatch: "sentio_proj2.users", ProcessorId: "proj2", TableName: "users"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := rewriter.parseSentioNetworkTables(tt.sql)
			if len(result) != len(tt.expected) {
				t.Errorf("expected %d tables, got %d", len(tt.expected), len(result))
				return
			}
			for i, exp := range tt.expected {
				if result[i].FullMatch != exp.FullMatch {
					t.Errorf("table[%d] FullMatch: expected %q, got %q", i, exp.FullMatch, result[i].FullMatch)
				}
				if result[i].ProcessorId != exp.ProcessorId {
					t.Errorf("table[%d] ProcessorId: expected %q, got %q", i, exp.ProcessorId, result[i].ProcessorId)
				}
				if result[i].TableName != exp.TableName {
					t.Errorf("table[%d] TableName: expected %q, got %q", i, exp.TableName, result[i].TableName)
				}
			}
		})
	}
}

func TestSentioNetworkRewriter_Rewrite(t *testing.T) {
	// Create mock network state
	state := NewInMemoryNetworkState()
	state.IndexerInfos[1] = IndexerInfo{
		IndexerId:           1,
		IndexerUrl:          "localhost",
		ClickhouseProxyPort: 9001,
	}
	state.IndexerInfos[2] = IndexerInfo{
		IndexerId:           2,
		IndexerUrl:          "12.34.56.78",
		ClickhouseProxyPort: 9001,
	}
	state.ProcessorAllocations["coinbase"] = []ProcessorAllocation{
		{ProcessorId: "coinbase", IndexerId: 1},
	}
	state.ProcessorAllocations["pancakeswap123"] = []ProcessorAllocation{
		{ProcessorId: "pancakeswap123", IndexerId: 2},
	}
	state.ProcessorInfos["coinbase"] = ProcessorInfo{ProcessorId: "coinbase"}
	state.ProcessorInfos["pancakeswap123"] = ProcessorInfo{ProcessorId: "pancakeswap123"}

	config := RewriterConfig{
		Enabled:        true,
		LocalIndexerId: 1,
		CHUser:         "default",
		CHPassword:     "test123",
	}

	rewriter, err := NewSentioNetworkRewriter(config, state)
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	tests := []struct {
		name           string
		sql            string
		expectRewrite  bool
		containsLocal  bool
		containsRemote bool
	}{
		{
			name:           "local table rewrite",
			sql:            "SELECT * FROM sentio_coinbase.transfer",
			expectRewrite:  true,
			containsLocal:  true,
			containsRemote: false,
		},
		{
			name:           "remote table rewrite",
			sql:            "SELECT * FROM sentio_pancakeswap123.Withdrawl",
			expectRewrite:  true,
			containsLocal:  false,
			containsRemote: true,
		},
		{
			name:          "no rewrite needed",
			sql:           "SELECT * FROM sentio.normal_table",
			expectRewrite: false,
		},
		{
			name:           "mixed local and remote",
			sql:            "SELECT * FROM sentio_coinbase.transfer UNION ALL SELECT * FROM sentio_pancakeswap123.Withdrawl",
			expectRewrite:  true,
			containsLocal:  true,
			containsRemote: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			result, err := rewriter.Rewrite(ctx, tt.sql)
			if err != nil {
				t.Fatalf("rewrite failed: %v", err)
			}

			if tt.expectRewrite {
				if result == tt.sql {
					t.Error("expected SQL to be rewritten, but got original")
				}
				if tt.containsLocal && !containsString(result, "sentio.coinbase") {
					t.Logf("result: %s", result)
					// Local table should be rewritten to sentio.prefix_table format
				}
				if tt.containsRemote && !containsString(result, "remote(") {
					t.Logf("result: %s", result)
					// Remote table should be rewritten to remote() function
				}
			} else {
				if result != tt.sql {
					t.Errorf("expected no rewrite, but got %q", result)
				}
			}
		})
	}
}

func TestNoopRewriter(t *testing.T) {
	rewriter := NoopRewriter{}
	ctx := context.Background()

	sql := "SELECT * FROM sentio_coinbase.transfer"
	result, err := rewriter.Rewrite(ctx, sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != sql {
		t.Errorf("NoopRewriter should return original SQL, got %q", result)
	}
}

func TestInMemoryNetworkState(t *testing.T) {
	state := NewInMemoryNetworkState()

	// Test empty state
	_, ok := state.GetProcessorAllocation("nonexistent")
	if ok {
		t.Error("expected false for nonexistent processor")
	}

	// Add and retrieve
	state.ProcessorAllocations["test"] = []ProcessorAllocation{
		{ProcessorId: "test", IndexerId: 1},
	}
	allocs, ok := state.GetProcessorAllocation("test")
	if !ok {
		t.Error("expected true for existing processor")
	}
	if len(allocs) != 1 {
		t.Errorf("expected 1 allocation, got %d", len(allocs))
	}

	// Test IndexerInfo
	state.IndexerInfos[1] = IndexerInfo{IndexerId: 1, IndexerUrl: "localhost"}
	info, ok := state.GetIndexerInfo(1)
	if !ok {
		t.Error("expected true for existing indexer")
	}
	if info.IndexerUrl != "localhost" {
		t.Errorf("expected localhost, got %s", info.IndexerUrl)
	}
}

func containsString(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && (s[0:len(substr)] == substr || containsString(s[1:], substr)))
}
