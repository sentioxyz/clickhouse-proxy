package proxy

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strings"
	"testing"
	"time"

	pb "github.com/sentioxyz/clickhouse-proxy/protos"

	"google.golang.org/grpc"
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
		{
			name:     "system tables ignored",
			sql:      "SELECT * FROM system.numbers",
			expected: nil,
		},
		{
			name: "mixed with system tables",
			sql:  "SELECT a.*, b.* FROM sentio_proj1.events a JOIN system.numbers b ON a.id = b.number",
			expected: []ParsedTable{
				{FullMatch: "sentio_proj1.events", ProcessorId: "proj1", TableName: "events"},
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

	mockAddr, stopMock := startMockRewriterService(t)
	defer stopMock()

	config := RewriterConfig{
		Enabled:     true,
		ServiceAddr: mockAddr,
		// Upstream matches coinbase's indexer (localhost:9001), so that
		// processor is treated as local; pancakeswap123 (12.34.56.78:9001)
		// falls on the remote branch.
		Upstream: "localhost:9001",
		Listen:   ":19001",
		Timeout:  2 * time.Second,
	}

	rewriter, err := NewSentioNetworkRewriter(config, state, networkV1MockTableRewriterFactory())
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
			result, err := rewriter.Rewrite(ctx, tt.sql, "default", "test123")
			if err != nil {
				t.Fatalf("rewrite failed: %v", err)
			}

			if tt.expectRewrite {
				if result == tt.sql {
					t.Error("expected SQL to be rewritten, but got original")
				}
				// NetworkV1: physical table name carries "${processorId}_${replica}."
				// inside a single identifier, so the rewritten SQL must mention
				// "coinbase_0.transfer" regardless of what ClickHouse DB wraps it.
				if tt.containsLocal && !containsString(result, "coinbase_0.transfer") {
					t.Errorf("expected local rewrite to reference coinbase_0.transfer, got %q", result)
				}
				if tt.containsRemote && !containsString(result, "remote(") {
					t.Errorf("expected remote rewrite via remote(), got %q", result)
				}
			} else {
				if result != tt.sql {
					t.Errorf("expected no rewrite, but got %q", result)
				}
			}
		})
	}
}

func TestFilterSentioNetworkTables(t *testing.T) {
	rewriter := &SentioNetworkRewriter{}

	astTables := []string{
		"sentio_coinbase.transfer",
		"system.numbers",
		"SYSTEM.processes",
		"pancakeswap.Withdrawl",
	}

	filtered := rewriter.filterSentioNetworkTables(astTables)

	// system.numbers and SYSTEM.processes are no longer filtered out;
	// they pass through and will be skipped later by NetworkState lookup.
	if len(filtered) != 4 {
		t.Errorf("expected 4 tables after filtering, got %d", len(filtered))
		for _, pt := range filtered {
			t.Logf("  %s", pt.FullMatch)
		}
	}

	expectedTables := map[string]bool{
		"sentio_coinbase.transfer": true,
		"system.numbers":           true,
		"SYSTEM.processes":         true,
		"pancakeswap.Withdrawl":    true,
	}

	for _, pt := range filtered {
		if !expectedTables[pt.FullMatch] {
			t.Errorf("unexpected table passed filter: %s", pt.FullMatch)
		}
	}
}

// mockTableRewriter is a test helper that returns explicit All() mappings.
type mockTableRewriter struct {
	database string
	mappings map[string]string // logical → physical
}

func (m *mockTableRewriter) Database() string { return m.database }
func (m *mockTableRewriter) RawTable(table string) (string, bool, error) {
	raw, ok := m.mappings[table]
	return raw, ok, nil
}
func (m *mockTableRewriter) RawTables(tables ...string) (map[string]string, error) {
	result := make(map[string]string)
	for _, t := range tables {
		if raw, ok := m.mappings[t]; ok {
			result[t] = raw
		}
	}
	return result, nil
}
func (m *mockTableRewriter) All() map[string]string { return m.mappings }
func (m *mockTableRewriter) Reverse(rawTable string) (string, bool, error) {
	for k, v := range m.mappings {
		if v == rawTable {
			return k, true, nil
		}
	}
	return "", false, nil
}

// mockTableRewriterFactory returns a test-only factory that creates a mockTableRewriter
// with identity mappings (table name passes through as-is). Replaces the removed DefaultTableRewriterFactory.
func mockTableRewriterFactory(database string) SentioNetworkTableRewriterFactory {
	return func(ctx context.Context, processorId string,
		indexerInfo IndexerInfo, processorInfo ProcessorInfo) (SentioNetworkTableRewriter, error) {
		// Return nil mappings — callers needing specific All() results should use a custom factory
		return &mockTableRewriter{
			database: database,
			mappings: nil,
		}, nil
	}
}

// networkV1MockTableRewriterFactory mimics NetworkV1 semantics:
//   - Database() is the ClickHouse connection DB (same across processors — "default" here).
//   - All() values bake the "${processorId}_${replica}." prefix into a single
//     identifier, because timeseries.Store.BuildTableNameWithoutDatabase
//     concatenates TableNamePrefix + TableSuffix (the period is part of the
//     raw table name, not a DB/table separator).
func networkV1MockTableRewriterFactory() SentioNetworkTableRewriterFactory {
	return func(ctx context.Context, processorId string,
		_ IndexerInfo, _ ProcessorInfo) (SentioNetworkTableRewriter, error) {
		prefix := fmt.Sprintf("%s_0.", processorId)
		return &mockTableRewriter{
			database: "default",
			mappings: map[string]string{
				"transfer":  prefix + "transfer",
				"Withdrawl": prefix + "Withdrawl",
			},
		}, nil
	}
}

// TestBuildRewriteMappings_UnknownProcessorSkipped verifies that when a processorId
// is not found in NetworkState (Redis), buildRewriteMappings silently skips it
// instead of returning an error. Known processors are still rewritten.
func TestBuildRewriteMappings_UnknownProcessorSkipped(t *testing.T) {
	state := NewInMemoryNetworkState()
	// Only "coinbase" is known
	state.IndexerInfos[1] = IndexerInfo{
		IndexerId:           1,
		IndexerUrl:          "localhost",
		ClickhouseProxyPort: 9001,
	}
	state.ProcessorAllocations["coinbase"] = []ProcessorAllocation{
		{ProcessorId: "coinbase", IndexerId: 1},
	}
	state.ProcessorInfos["coinbase"] = ProcessorInfo{ProcessorId: "coinbase"}

	// Use a mock factory that returns explicit All() mappings
	mockFactory := func(ctx context.Context, processorId string,
		indexerInfo IndexerInfo, processorInfo ProcessorInfo) (SentioNetworkTableRewriter, error) {
		return &mockTableRewriter{
			database: "default",
			mappings: map[string]string{
				"transfer": "coinbase_0.transfer",
			},
		}, nil
	}

	rewriter := &SentioNetworkRewriter{
		config:               RewriterConfig{Upstream: "localhost:9001"},
		networkState:         state,
		tableRewriterFactory: mockFactory,
	}

	tables := []ParsedTable{
		{FullMatch: "sentio_coinbase.transfer", ProcessorId: "coinbase", TableName: "transfer"},
		{FullMatch: "system.numbers", ProcessorId: "system", TableName: "numbers"},               // unknown
		{FullMatch: "unknown_db.some_table", ProcessorId: "unknown_db", TableName: "some_table"}, // unknown
	}

	ctx := context.Background()
	tableWithDB, remoteTable, err := rewriter.buildRewriteMappings(ctx, tables, "default", "pass")
	if err != nil {
		t.Fatalf("expected no error for unknown processors, got: %v", err)
	}

	// "coinbase" is local (upstream matches), so should appear in tableWithDB
	if _, ok := tableWithDB["sentio_coinbase.transfer"]; !ok {
		t.Error("expected sentio_coinbase.transfer in tableWithDatabaseMap")
	}

	// Unknown processors should NOT appear in either map
	if _, ok := tableWithDB["system.numbers"]; ok {
		t.Error("system.numbers should not be in tableWithDatabaseMap")
	}
	if _, ok := remoteTable["system.numbers"]; ok {
		t.Error("system.numbers should not be in remoteTableMap")
	}
	if _, ok := tableWithDB["unknown_db.some_table"]; ok {
		t.Error("unknown_db.some_table should not be in tableWithDatabaseMap")
	}
	if _, ok := remoteTable["unknown_db.some_table"]; ok {
		t.Error("unknown_db.some_table should not be in remoteTableMap")
	}
}

// TestBuildRewriteMappings_AllMappings verifies that buildRewriteMappings uses All()
// to get all physical table mappings, and tables not in the mapping are silently skipped.
func TestBuildRewriteMappings_AllMappings(t *testing.T) {
	state := NewInMemoryNetworkState()
	state.IndexerInfos[1] = IndexerInfo{
		IndexerId:           1,
		IndexerUrl:          "localhost",
		ClickhouseProxyPort: 9001,
	}
	state.ProcessorAllocations["myproc"] = []ProcessorAllocation{
		{ProcessorId: "myproc", IndexerId: 1},
	}
	state.ProcessorInfos["myproc"] = ProcessorInfo{ProcessorId: "myproc"}

	// Mock factory (NetworkV1): Database() is the CH connection DB ("default"),
	// physical table names carry the "${processorId}_${replica}." prefix inside
	// a single identifier (matches timeseries.Store.BuildTableNameWithoutDatabase).
	// "unknown_table" is NOT in the mapping.
	mockFactory := func(ctx context.Context, processorId string,
		indexerInfo IndexerInfo, processorInfo ProcessorInfo) (SentioNetworkTableRewriter, error) {
		return &mockTableRewriter{
			database: "default",
			mappings: map[string]string{
				"transfer": "myproc_0.event_Transfer",
				"orders":   "myproc_0.event_Orders",
			},
		}, nil
	}

	rewriter := &SentioNetworkRewriter{
		config:               RewriterConfig{Upstream: "localhost:9001"},
		networkState:         state,
		tableRewriterFactory: mockFactory,
	}

	tables := []ParsedTable{
		{FullMatch: "sentio_myproc.transfer", ProcessorId: "myproc", TableName: "transfer"},
		{FullMatch: "sentio_myproc.orders", ProcessorId: "myproc", TableName: "orders"},
		{FullMatch: "sentio_myproc.unknown_table", ProcessorId: "myproc", TableName: "unknown_table"},
	}

	ctx := context.Background()
	tableWithDB, _, err := rewriter.buildRewriteMappings(ctx, tables, "default", "pass")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// "transfer" and "orders" should be rewritten: Database stays the CH
	// connection DB, Table carries the processor/replica prefix.
	if td, ok := tableWithDB["sentio_myproc.transfer"]; !ok {
		t.Error("expected sentio_myproc.transfer in tableWithDatabaseMap")
	} else {
		if td.Database != "default" {
			t.Errorf("expected database default, got %s", td.Database)
		}
		if td.Table != "myproc_0.event_Transfer" {
			t.Errorf("expected physical table myproc_0.event_Transfer, got %s", td.Table)
		}
	}
	if td, ok := tableWithDB["sentio_myproc.orders"]; !ok {
		t.Error("expected sentio_myproc.orders in tableWithDatabaseMap")
	} else {
		if td.Database != "default" {
			t.Errorf("expected database default, got %s", td.Database)
		}
		if td.Table != "myproc_0.event_Orders" {
			t.Errorf("expected physical table myproc_0.event_Orders, got %s", td.Table)
		}
	}

	// "unknown_table" should NOT be in the map (silently skipped)
	if _, ok := tableWithDB["sentio_myproc.unknown_table"]; ok {
		t.Error("unknown_table should not be in tableWithDatabaseMap, expected it to be skipped")
	}
}

func TestNoopRewriter(t *testing.T) {
	rewriter := NoopRewriter{}
	ctx := context.Background()

	sql := "SELECT * FROM sentio_coinbase.transfer"
	result, err := rewriter.Rewrite(ctx, sql, "default", "test123")
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

func TestResolveCallbackAddr(t *testing.T) {
	tests := []struct {
		name           string
		upstream       string
		listen         string
		expectPort     string // expected port suffix
		expectNonLocal bool   // true = should NOT be "localhost"
	}{
		{
			name:           "remote upstream should auto-detect IP",
			upstream:       "10.15.0.100:9000",
			listen:         ":9001",
			expectPort:     "9001",
			expectNonLocal: true,
		},
		{
			name:           "empty upstream falls back to localhost",
			upstream:       "",
			listen:         ":9001",
			expectPort:     "9001",
			expectNonLocal: false,
		},
		{
			name:           "listen with host:port format",
			upstream:       "10.15.0.100:9000",
			listen:         "0.0.0.0:22200",
			expectPort:     "22200",
			expectNonLocal: true,
		},
		{
			name:           "listen port only",
			upstream:       "8.8.8.8:53",
			listen:         ":8080",
			expectPort:     "8080",
			expectNonLocal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := resolveCallbackAddr(tt.upstream, tt.listen)
			t.Logf("resolveCallbackAddr(%q, %q) = %q", tt.upstream, tt.listen, result)

			// Must end with correct port
			if !strings.HasSuffix(result, ":"+tt.expectPort) {
				t.Errorf("expected port %s, got %s", tt.expectPort, result)
			}

			// Check localhost vs non-localhost
			isLocal := strings.HasPrefix(result, "localhost:")
			if tt.expectNonLocal && isLocal {
				t.Errorf("expected non-localhost address for upstream=%q, got %s", tt.upstream, result)
			}
			if !tt.expectNonLocal && !isLocal {
				t.Errorf("expected localhost fallback for upstream=%q, got %s", tt.upstream, result)
			}
		})
	}
}

// mockRewriterServer is a minimal in-process gRPC implementation of
// RewriterServiceServer used by TestSentioNetworkRewriter_Rewrite. It is not
// trying to faithfully reproduce the C++ sql-rewriter; it only supplies the
// two behaviors SentioNetworkRewriter depends on:
//
//	Phase 1 (Options == nil): return all "db.table" references in the SQL as
//	                          OriginalAccessedTableNames.
//	Phase 2 (TableNameRewrite): apply the TableWithDatabaseMap / RemoteTableMap
//	                            substitutions with plain ReplaceAll.
type mockRewriterServer struct {
	pb.UnimplementedRewriterServiceServer
}

var mockTableRefPattern = regexp.MustCompile(`[A-Za-z_][\w-]*\.[A-Za-z_][\w-]*`)

func (s *mockRewriterServer) Rewrite(_ context.Context, req *pb.RewriteSQLRequest) (*pb.RewriteSQLResponse, error) {
	if len(req.Options) == 0 {
		seen := make(map[string]bool)
		var names []string
		for _, m := range mockTableRefPattern.FindAllString(req.Sql, -1) {
			if seen[m] {
				continue
			}
			seen[m] = true
			names = append(names, m)
		}
		return &pb.RewriteSQLResponse{
			Code:                       pb.RewriteCode_Success,
			OriginalAccessedTableNames: names,
		}, nil
	}

	out := req.Sql
	for _, opt := range req.Options {
		if opt.Op != pb.RewriteOp_TableNameRewrite {
			continue
		}
		args := opt.GetTableNameArgs()
		if args == nil {
			continue
		}
		for original, td := range args.TableWithDatabaseMap {
			out = strings.ReplaceAll(out, original, fmt.Sprintf("%s.%s", td.Database, td.Table))
		}
		for original, rt := range args.RemoteTableMap {
			out = strings.ReplaceAll(out, original,
				fmt.Sprintf("remote('%s', '%s', '%s', '%s', '%s')",
					rt.Addr, rt.Database, rt.Table, rt.User, rt.Password))
		}
	}
	return &pb.RewriteSQLResponse{
		Code:            pb.RewriteCode_Success,
		SqlAfterRewrite: out,
	}, nil
}

// startMockRewriterService launches an in-process gRPC rewriter on 127.0.0.1:0
// and returns its address plus a stop function. SentioNetworkRewriter uses
// grpc.DialContext with WithBlock, so a real TCP listener is the simplest way
// to give it something to connect to.
func startMockRewriterService(t *testing.T) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen for mock rewriter: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterRewriterServiceServer(srv, &mockRewriterServer{})
	go func() { _ = srv.Serve(lis) }()
	return lis.Addr().String(), func() {
		srv.Stop()
	}
}
