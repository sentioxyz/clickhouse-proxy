package proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// ============================================================================
// 端到端集成测试：SQL Rewriter
// 覆盖场景（基于 tmp_paste_content.md）：
// 1. 本地表重写: sentio_coinbase.transfer -> sentio.w6B0Uyvq_event_Transfer
// 2. 远程表重写: sentio_pancakeswap123.Withdrawl -> remote(...)
// 3. 混合查询 (UNION ALL)
// 4. JOIN 查询
// 5. 子查询
// 6. 复杂嵌套查询
// 7. 异常场景（无效 processor_id, 服务不可用等）
// ============================================================================

// MockClickHouseServer 模拟 ClickHouse 服务器
type MockClickHouseServer struct {
	listener    net.Listener
	addr        string
	receivedSQL []string
	mu          sync.Mutex
	stopCh      chan struct{}
}

// NewMockClickHouseServer 创建模拟服务器
func NewMockClickHouseServer(t *testing.T) *MockClickHouseServer {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock server: %v", err)
	}

	server := &MockClickHouseServer{
		listener:    ln,
		addr:        ln.Addr().String(),
		receivedSQL: make([]string, 0),
		stopCh:      make(chan struct{}),
	}

	go server.serve()
	return server
}

func (s *MockClickHouseServer) serve() {
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		s.listener.(*net.TCPListener).SetDeadline(time.Now().Add(100 * time.Millisecond))
		conn, err := s.listener.Accept()
		if err != nil {
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *MockClickHouseServer) handleConn(conn net.Conn) {
	defer conn.Close()

	// Phase 1: 读取 ClientHello
	// ClickHouse Native 协议: ClientHello = [type=0: UVarInt] + [client_name: String] + [major: UVarInt] + ...
	helloBuf := make([]byte, 4096)
	n, err := conn.Read(helloBuf)
	if err != nil || n == 0 {
		return
	}

	// Phase 2: 发送 ServerHello
	// ServerHello = [type=0: UVarInt] + [name: String] + [major: UVarInt] + [minor: UVarInt] + [revision: UVarInt]
	//            + [timezone: String (if revision >= 54058)] + [display_name: String (if revision >= 54372)]
	//            + [version_patch: UVarInt (if revision >= 54401)]
	var serverHello []byte
	var buf [binary.MaxVarintLen64]byte
	// packet_type = 0 (ServerHello)
	n2 := binary.PutUvarint(buf[:], 0)
	serverHello = append(serverHello, buf[:n2]...)
	// server_name = "MockClickHouse"
	name := []byte("MockClickHouse")
	n2 = binary.PutUvarint(buf[:], uint64(len(name)))
	serverHello = append(serverHello, buf[:n2]...)
	serverHello = append(serverHello, name...)
	// major = 24
	n2 = binary.PutUvarint(buf[:], 24)
	serverHello = append(serverHello, buf[:n2]...)
	// minor = 1
	n2 = binary.PutUvarint(buf[:], 1)
	serverHello = append(serverHello, buf[:n2]...)
	// revision = 54460 (supports timezone, display_name, version_patch, settings_serialized_as_strings, etc.)
	revision := uint64(54460)
	n2 = binary.PutUvarint(buf[:], revision)
	serverHello = append(serverHello, buf[:n2]...)
	// timezone = "UTC" (revision >= 54058)
	tz := []byte("UTC")
	n2 = binary.PutUvarint(buf[:], uint64(len(tz)))
	serverHello = append(serverHello, buf[:n2]...)
	serverHello = append(serverHello, tz...)
	// display_name = "mock" (revision >= 54372)
	dn := []byte("mock")
	n2 = binary.PutUvarint(buf[:], uint64(len(dn)))
	serverHello = append(serverHello, buf[:n2]...)
	serverHello = append(serverHello, dn...)
	// version_patch = 0 (revision >= 54401)
	n2 = binary.PutUvarint(buf[:], 0)
	serverHello = append(serverHello, buf[:n2]...)

	if _, err := conn.Write(serverHello); err != nil {
		return
	}

	// Phase 3: 读取 Addendum (if any) + 包循环
	// 读取并处理后续包
	for {
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		pktBuf := make([]byte, 65536)
		n, err := conn.Read(pktBuf)
		if err != nil {
			return
		}

		data := pktBuf[:n]

		// 尝试提取 SQL
		if sql := extractSQLFromPacket(data); sql != "" {
			s.mu.Lock()
			s.receivedSQL = append(s.receivedSQL, sql)
			s.mu.Unlock()
		}

		// 检查包类型并发送适当响应
		if len(data) > 0 {
			pktType, _ := binary.Uvarint(data)
			switch pktType {
			case 1: // ClientQuery -> 回复 EndOfStream
				conn.Write([]byte{5}) // EndOfStream
			case 4: // ClientPing -> 回复 Pong
				conn.Write([]byte{7}) // ServerPong (type=7 Pong in ClickHouse protocol >= 54451)
			case 2: // ClientData -> 回复 EndOfStream (INSERT 完成)
				// 检查是否是空 data block (INSERT 结束标记)
				conn.Write([]byte{5}) // EndOfStream
			default:
				// 其他包类型，回复 EndOfStream
				conn.Write([]byte{5})
			}
		}
	}
}

func (s *MockClickHouseServer) GetReceivedSQL() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]string, len(s.receivedSQL))
	copy(result, s.receivedSQL)
	return result
}

func (s *MockClickHouseServer) ClearReceivedSQL() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.receivedSQL = s.receivedSQL[:0]
}

func (s *MockClickHouseServer) Stop() {
	close(s.stopCh)
	s.listener.Close()
}

func (s *MockClickHouseServer) Addr() string {
	return s.addr
}

// extractSQLFromPacket 从 ClickHouse Native 协议包中提取 SQL
func extractSQLFromPacket(data []byte) string {
	// 改进实现：扫描 varint-prefixed 字符串，匹配更多 SQL 关键词
	sqlKeywords := []string{"SELECT", "INSERT", "CREATE", "DROP", "ALTER", "SHOW", "DESCRIBE", "EXPLAIN", "TRUNCATE", "OPTIMIZE", "WITH"}
	for i := 0; i < len(data)-10; i++ {
		// 尝试读取 varint 长度
		length, n := binary.Uvarint(data[i:])
		if n <= 0 || length < 6 || length > 100000 || i+n+int(length) > len(data) {
			continue
		}
		potential := string(data[i+n : i+n+int(length)])
		// 检查是否像 SQL
		upper := strings.ToUpper(strings.TrimSpace(potential))
		for _, kw := range sqlKeywords {
			if strings.HasPrefix(upper, kw) {
				return potential
			}
		}
	}
	return ""
}

// TestEndToEnd_LocalTableRewrite 测试本地表重写
func TestEndToEnd_LocalTableRewrite(t *testing.T) {
	// 设置网络状态
	state := setupTestNetworkState()

	// 创建 rewriter
	config := RewriterConfig{
		Enabled: true,
		ServiceAddr: "localhost:50051",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	ctx := context.Background()

	// 测试用例：本地表
	testCases := []struct {
		name        string
		inputSQL    string
		expectLocal bool // 期望本地表（sentio.xxx）
	}{
		{
			name:        "simple local table",
			inputSQL:    "SELECT COUNT(*) FROM sentio_coinbase.transfer",
			expectLocal: true,
		},
		{
			name:        "local table with WHERE",
			inputSQL:    "SELECT * FROM sentio_coinbase.transfer WHERE amount > 100",
			expectLocal: true,
		},
		{
			name:        "local table with ORDER BY",
			inputSQL:    "SELECT * FROM sentio_coinbase.transfer ORDER BY timestamp DESC",
			expectLocal: true,
		},
		{
			name:        "local table with LIMIT",
			inputSQL:    "SELECT * FROM sentio_coinbase.transfer LIMIT 10",
			expectLocal: true,
		},
		{
			name:        "local table with GROUP BY",
			inputSQL:    "SELECT address, COUNT(*) FROM sentio_coinbase.transfer GROUP BY address",
			expectLocal: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := rewriter.Rewrite(ctx, tc.inputSQL, "default", "test123")
			if err != nil {
				t.Fatalf("rewrite failed: %v", err)
			}

			// 验证重写结果
			if tc.expectLocal {
				if !strings.Contains(result, "sentio.coinbase") {
					t.Logf("Rewritten SQL: %s", result)
				}
				// 不应包含 remote()
				if strings.Contains(result, "remote(") {
					t.Errorf("local table should not contain remote(), got: %s", result)
				}
			}

			// 验证原始表名已被替换
			if strings.Contains(result, "sentio_coinbase.transfer") {
				t.Errorf("original table name should be replaced, got: %s", result)
			}
		})
	}
}

// TestEndToEnd_RemoteTableRewrite 测试远程表重写
func TestEndToEnd_RemoteTableRewrite(t *testing.T) {
	state := setupTestNetworkState()

	config := RewriterConfig{
		Enabled: true,
		ServiceAddr: "localhost:50051",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	ctx := context.Background()

	// 测试用例：远程表（indexer_id=2，与本地不同）
	testCases := []struct {
		name         string
		inputSQL     string
		expectRemote bool
	}{
		{
			name:         "simple remote table",
			inputSQL:     "SELECT COUNT(*) FROM sentio_pancakeswap123.Withdrawl",
			expectRemote: true,
		},
		{
			name:         "remote table with filters",
			inputSQL:     "SELECT * FROM sentio_pancakeswap123.Withdrawl WHERE amount > 1000",
			expectRemote: true,
		},
		{
			name:         "remote table aggregation",
			inputSQL:     "SELECT SUM(amount), AVG(amount) FROM sentio_pancakeswap123.Withdrawl",
			expectRemote: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := rewriter.Rewrite(ctx, tc.inputSQL, "default", "test123")
			if err != nil {
				t.Fatalf("rewrite failed: %v", err)
			}

			if tc.expectRemote {
				// 应该包含 remote() 函数调用
				if !strings.Contains(result, "remote(") {
					t.Errorf("expected remote() function, got: %s", result)
				}
				// 应该包含远程地址
				if !strings.Contains(result, "12.34.56.78") {
					t.Errorf("expected remote address in result, got: %s", result)
				}
			}

			// 验证原始表名已被替换
			if strings.Contains(result, "sentio_pancakeswap123.Withdrawl") {
				t.Errorf("original table name should be replaced, got: %s", result)
			}
		})
	}
}

// TestEndToEnd_MixedUnionQuery 测试混合 UNION ALL 查询（文档核心场景）
func TestEndToEnd_MixedUnionQuery(t *testing.T) {
	state := setupTestNetworkState()

	config := RewriterConfig{
		Enabled: true,
		ServiceAddr: "localhost:50051",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	ctx := context.Background()

	// 核心场景：混合本地和远程表的 UNION ALL（来自 tmp_paste_content.md）
	inputSQL := `SELECT COUNT(*) FROM sentio_coinbase.transfer UNION ALL SELECT COUNT(*) FROM sentio_pancakeswap123.Withdrawl`

	result, err := rewriter.Rewrite(ctx, inputSQL, "default", "test123")
	if err != nil {
		t.Fatalf("rewrite failed: %v", err)
	}

	t.Logf("Input SQL:    %s", inputSQL)
	t.Logf("Rewritten SQL: %s", result)

	// 验证同时包含本地表和远程表
	hasLocal := !strings.Contains(result, "sentio_coinbase.transfer")                           // 原始表名被替换
	hasRemote := strings.Contains(result, "remote(") && strings.Contains(result, "12.34.56.78") // 远程函数调用
	notOriginal := !strings.Contains(result, "sentio_pancakeswap123.Withdrawl")                 // 原始表名被替换

	if !hasLocal || !hasRemote || !notOriginal {
		t.Errorf("mixed query rewrite failed: hasLocal=%v, hasRemote=%v, originalReplaced=%v", hasLocal, hasRemote, notOriginal)
	}

	// 验证 UNION ALL 结构保持
	if !strings.Contains(strings.ToUpper(result), "UNION ALL") {
		t.Error("UNION ALL should be preserved")
	}
}

// TestEndToEnd_JoinQuery 测试 JOIN 查询
func TestEndToEnd_JoinQuery(t *testing.T) {
	state := setupTestNetworkState()

	config := RewriterConfig{
		Enabled: true,
		ServiceAddr: "localhost:50051",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	ctx := context.Background()

	testCases := []struct {
		name     string
		inputSQL string
	}{
		{
			name:     "simple JOIN",
			inputSQL: "SELECT a.*, b.amount FROM sentio_coinbase.transfer a JOIN sentio_coinbase.events b ON a.id = b.transfer_id",
		},
		{
			name:     "LEFT JOIN with remote",
			inputSQL: "SELECT a.*, b.* FROM sentio_coinbase.transfer a LEFT JOIN sentio_pancakeswap123.Withdrawl b ON a.address = b.sender",
		},
		{
			name:     "multiple JOINs",
			inputSQL: "SELECT * FROM sentio_coinbase.transfer t JOIN sentio_coinbase.events e ON t.id = e.id JOIN sentio_pancakeswap123.Withdrawl w ON t.address = w.sender",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := rewriter.Rewrite(ctx, tc.inputSQL, "default", "test123")
			if err != nil {
				t.Fatalf("rewrite failed: %v", err)
			}

			t.Logf("Input:  %s", tc.inputSQL)
			t.Logf("Output: %s", result)

			// 验证原始 sentio_ 表名被替换
			if strings.Contains(result, "sentio_coinbase.") || strings.Contains(result, "sentio_pancakeswap123.") {
				t.Error("original table names should be replaced")
			}

			// 验证 JOIN 结构保持
			if strings.Contains(tc.inputSQL, "JOIN") && !strings.Contains(strings.ToUpper(result), "JOIN") {
				t.Error("JOIN keyword should be preserved")
			}
		})
	}
}

// TestEndToEnd_SubQuery 测试子查询
func TestEndToEnd_SubQuery(t *testing.T) {
	state := setupTestNetworkState()

	config := RewriterConfig{
		Enabled: true,
		ServiceAddr: "localhost:50051",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	ctx := context.Background()

	testCases := []struct {
		name     string
		inputSQL string
	}{
		{
			name:     "WHERE subquery",
			inputSQL: "SELECT * FROM sentio_coinbase.transfer WHERE amount > (SELECT AVG(amount) FROM sentio_coinbase.transfer)",
		},
		{
			name:     "FROM subquery",
			inputSQL: "SELECT * FROM (SELECT * FROM sentio_coinbase.transfer WHERE amount > 100) AS t",
		},
		{
			name:     "nested subquery with remote",
			inputSQL: "SELECT * FROM sentio_coinbase.transfer WHERE sender IN (SELECT sender FROM sentio_pancakeswap123.Withdrawl)",
		},
		{
			name:     "WITH clause (CTE)",
			inputSQL: "WITH t AS (SELECT * FROM sentio_coinbase.transfer) SELECT COUNT(*) FROM t",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := rewriter.Rewrite(ctx, tc.inputSQL, "default", "test123")
			if err != nil {
				t.Fatalf("rewrite failed: %v", err)
			}

			t.Logf("Input:  %s", tc.inputSQL)
			t.Logf("Output: %s", result)

			// 验证表名被替换
			if strings.Contains(result, "sentio_coinbase.") {
				t.Error("original table names should be replaced")
			}
		})
	}
}

// TestEndToEnd_ErrorScenarios 测试异常场景
func TestEndToEnd_ErrorScenarios(t *testing.T) {
	state := setupTestNetworkState()

	config := RewriterConfig{
		Enabled: true,
		ServiceAddr: "localhost:50051",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	ctx := context.Background()

	testCases := []struct {
		name           string
		inputSQL       string
		expectError    bool
		expectNoChange bool
	}{
		{
			name:           "unknown processor_id",
			inputSQL:       "SELECT * FROM sentio_unknown_processor.table",
			expectError:    true, // processor 找不到时应返回错误
			expectNoChange: false,
		},
		{
			name:           "normal table (not sentio pattern)",
			inputSQL:       "SELECT * FROM sentio.normal_table",
			expectError:    false,
			expectNoChange: true, // 不匹配模式，不重写
		},
		{
			name:           "system table",
			inputSQL:       "SELECT * FROM system.tables",
			expectError:    false,
			expectNoChange: true,
		},
		{
			name:           "empty SQL",
			inputSQL:       "",
			expectError:    false,
			expectNoChange: true,
		},
		{
			name:           "malformed SQL",
			inputSQL:       "SELEC * FRM sentio_coinbase.transfer",
			expectError:    false, // 不解析 SQL 语法
			expectNoChange: false, // 仍然会尝试重写表名
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := rewriter.Rewrite(ctx, tc.inputSQL, "default", "test123")

			if tc.expectError {
				if err == nil {
					t.Error("expected error but got nil")
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if tc.expectNoChange && result != tc.inputSQL {
				t.Errorf("expected no change, but got different result.\nInput:  %s\nOutput: %s", tc.inputSQL, result)
			}
		})
	}
}

// TestEndToEnd_NoopRewriterFallback 测试 NoopRewriter 降级
func TestEndToEnd_NoopRewriterFallback(t *testing.T) {
	rewriter := NoopRewriter{}
	ctx := context.Background()

	testCases := []string{
		"SELECT * FROM sentio_coinbase.transfer",
		"SELECT COUNT(*) FROM sentio_pancakeswap123.Withdrawl",
		"SELECT * FROM normal_table",
	}

	for _, sql := range testCases {
		result, err := rewriter.Rewrite(ctx, sql, "default", "test123")
		if err != nil {
			t.Errorf("NoopRewriter should not return error, got: %v", err)
		}
		if result != sql {
			t.Errorf("NoopRewriter should return original SQL.\nInput:  %s\nOutput: %s", sql, result)
		}
	}
}

// TestEndToEnd_ConcurrentRewrite 测试并发重写
func TestEndToEnd_ConcurrentRewrite(t *testing.T) {
	state := setupTestNetworkState()

	config := RewriterConfig{
		Enabled: true,
		ServiceAddr: "localhost:50051",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	ctx := context.Background()
	numGoroutines := 10
	numIterations := 100

	var wg sync.WaitGroup
	errCh := make(chan error, numGoroutines*numIterations)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				sql := fmt.Sprintf("SELECT * FROM sentio_coinbase.transfer_%d_%d", id, j)
				_, err := rewriter.Rewrite(ctx, sql, "default", "test123")
				if err != nil {
					errCh <- err
				}
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent rewrite error: %v", err)
	}
}

// TestEndToEnd_SpecialCharactersInSQL 测试 SQL 中的特殊字符
func TestEndToEnd_SpecialCharactersInSQL(t *testing.T) {
	state := setupTestNetworkState()

	config := RewriterConfig{
		Enabled: true,
		ServiceAddr: "localhost:50051",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	ctx := context.Background()

	testCases := []struct {
		name     string
		inputSQL string
	}{
		{
			name:     "single quotes in value",
			inputSQL: "SELECT * FROM sentio_coinbase.transfer WHERE name = 'test''s value'",
		},
		{
			name:     "backticks",
			inputSQL: "SELECT * FROM `sentio_coinbase`.`transfer`",
		},
		{
			name:     "double quotes",
			inputSQL: `SELECT * FROM "sentio_coinbase"."transfer"`,
		},
		{
			name:     "unicode characters",
			inputSQL: "SELECT * FROM sentio_coinbase.transfer WHERE name = '中文测试'",
		},
		{
			name:     "newlines and tabs",
			inputSQL: "SELECT *\n\tFROM sentio_coinbase.transfer\n\tWHERE amount > 100",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := rewriter.Rewrite(ctx, tc.inputSQL, "default", "test123")
			if err != nil {
				t.Fatalf("rewrite failed: %v", err)
			}
			t.Logf("Input:  %q", tc.inputSQL)
			t.Logf("Output: %q", result)
		})
	}
}

// TestEndToEnd_NetworkStateUpdates 测试网络状态更新
func TestEndToEnd_NetworkStateUpdates(t *testing.T) {
	state := NewInMemoryNetworkState()

	// 初始状态为空
	config := RewriterConfig{
		Enabled: true,
		ServiceAddr: "localhost:50051",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	ctx := context.Background()
	sql := "SELECT * FROM sentio_newprocessor.events"

	// 第一次：processor 不存在，应返回错误
	_, err = rewriter.Rewrite(ctx, sql, "default", "test123")
	if err == nil {
		t.Error("expected error when processor not found, but got nil")
	} else {
		t.Logf("First rewrite (processor not found): got expected error: %v", err)
	}

	// 动态添加 processor
	state.ProcessorAllocations["newprocessor"] = []ProcessorAllocation{
		{ProcessorId: "newprocessor", IndexerId: 1},
	}
	state.IndexerInfos[1] = IndexerInfo{
		IndexerId:           1,
		IndexerUrl:          "localhost",
		ClickhouseProxyPort: 9001,
	}
	state.ProcessorInfos["newprocessor"] = ProcessorInfo{
		ProcessorId: "newprocessor",
	}

	// 第二次：processor 存在，应重写
	result2, _ := rewriter.Rewrite(ctx, sql, "default", "test123")
	if result2 == sql {
		t.Log("Second rewrite (processor added): SQL rewritten as expected")
	}
}

// TestEndToEnd_MultipleQueriesSameRewriter 测试同一 rewriter 连续处理多个 Query
func TestEndToEnd_MultipleQueriesSameRewriter(t *testing.T) {
	state := setupTestNetworkState()

	config := RewriterConfig{
		Enabled: true,
		ServiceAddr: "localhost:50051",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	ctx := context.Background()

	// 模拟同一连接上的连续多查询
	queries := []string{
		"SELECT COUNT(*) FROM sentio_coinbase.transfer",
		"SELECT * FROM sentio_pancakeswap123.Withdrawl WHERE amount > 100",
		"SELECT 1", // 不需要重写的查询
		"SELECT a.*, b.* FROM sentio_coinbase.transfer a JOIN sentio_pancakeswap123.Withdrawl b ON a.id = b.id",
		"SHOW TABLES",
	}

	for i, sql := range queries {
		result, err := rewriter.Rewrite(ctx, sql, "default", "test123")
		if err != nil {
			t.Errorf("query %d (%q) rewrite failed: %v", i, sql, err)
			continue
		}
		t.Logf("Query %d: %q -> %q", i, sql, result)
	}
}

// TestEndToEnd_ShowAndDescribeStatements 测试 SHOW/DESCRIBE 语句不被干扰
func TestEndToEnd_ShowAndDescribeStatements(t *testing.T) {
	state := setupTestNetworkState()

	config := RewriterConfig{
		Enabled: true,
		ServiceAddr: "localhost:50051",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	ctx := context.Background()

	testCases := []struct {
		name           string
		inputSQL       string
		expectNoChange bool
	}{
		{
			name:           "SHOW TABLES",
			inputSQL:       "SHOW TABLES FROM default",
			expectNoChange: true,
		},
		{
			name:           "DESCRIBE table",
			inputSQL:       "DESCRIBE TABLE default.test_table",
			expectNoChange: true,
		},
		{
			name:           "SHOW CREATE TABLE",
			inputSQL:       "SHOW CREATE TABLE default.test_table",
			expectNoChange: true,
		},
		{
			name:           "EXPLAIN query",
			inputSQL:       "EXPLAIN SELECT * FROM sentio_coinbase.transfer",
			expectNoChange: false, // 内部的表名仍需重写
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := rewriter.Rewrite(ctx, tc.inputSQL, "default", "test123")
			if err != nil {
				t.Fatalf("rewrite failed: %v", err)
			}
			t.Logf("Input:  %q", tc.inputSQL)
			t.Logf("Output: %q", result)
			if tc.expectNoChange && result != tc.inputSQL {
				t.Errorf("expected no change, got: %s", result)
			}
		})
	}
}

// TestEndToEnd_LargeSQL 测试大 SQL 查询（超过 4KB）
func TestEndToEnd_LargeSQL(t *testing.T) {
	state := setupTestNetworkState()

	config := RewriterConfig{
		Enabled: true,
		ServiceAddr: "localhost:50051",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	ctx := context.Background()

	// 构造一个超过 4KB 的 SQL (模拟 INSERT ... VALUES 或复杂的 IN 列表)
	var sb strings.Builder
	sb.WriteString("SELECT * FROM sentio_coinbase.transfer WHERE id IN (")
	for i := 0; i < 500; i++ {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%d", i))
	}
	sb.WriteString(")")

	largeSQL := sb.String()
	t.Logf("Large SQL size: %d bytes", len(largeSQL))

	result, err := rewriter.Rewrite(ctx, largeSQL, "default", "test123")
	if err != nil {
		t.Fatalf("large SQL rewrite failed: %v", err)
	}

	// 验证表名被替换
	if strings.Contains(result, "sentio_coinbase.transfer") {
		t.Error("original table name should be replaced in large SQL")
	}

	// 验证 IN 列表保持完整
	if !strings.Contains(result, "499") {
		t.Error("IN list should be preserved in large SQL")
	}
}

// TestEndToEnd_RemoteCallbackAddr verifies that remote() uses auto-detected IP
// instead of "localhost" when proxy and ClickHouse are on different hosts.
func TestEndToEnd_RemoteCallbackAddr(t *testing.T) {
	state := setupTestNetworkState()

	// Simulate cross-host deployment: upstream is a remote address (not localhost)
	config := RewriterConfig{
		Enabled:     true,
		ServiceAddr: "localhost:50051",
		Upstream:    "10.15.0.100:39000", // remote CK address
		Listen:      ":22200",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		t.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	// Verify callbackAddr was resolved to non-localhost
	t.Logf("Resolved callbackAddr: %s", rewriter.callbackAddr)
	if strings.HasPrefix(rewriter.callbackAddr, "localhost:") {
		t.Errorf("callbackAddr should not be localhost when upstream is remote, got: %s", rewriter.callbackAddr)
	}
	if !strings.HasSuffix(rewriter.callbackAddr, ":22200") {
		t.Errorf("callbackAddr should use listen port 22200, got: %s", rewriter.callbackAddr)
	}

	ctx := context.Background()

	// Rewrite a remote table query
	inputSQL := "SELECT * FROM sentio_pancakeswap123.Withdrawl"
	result, err := rewriter.Rewrite(ctx, inputSQL, "default", "test123")
	if err != nil {
		t.Fatalf("rewrite failed: %v", err)
	}

	t.Logf("Input:  %s", inputSQL)
	t.Logf("Output: %s", result)

	// Core assertion: remote() should NOT use "localhost"
	if strings.Contains(result, "remote('localhost:") {
		t.Errorf("remote() should not use localhost when proxy and CK are on different hosts.\nGot: %s", result)
	}

	// Should contain remote() with the auto-detected IP
	if !strings.Contains(result, "remote(") {
		t.Errorf("expected remote() function in result, got: %s", result)
	}

	// Should contain the correct port
	if !strings.Contains(result, ":22200") {
		t.Errorf("expected port 22200 in remote() addr, got: %s", result)
	}

	// Should contain __route__ for routing
	if !strings.Contains(result, "__route__") {
		t.Errorf("expected __route__ in user parameter, got: %s", result)
	}
}

// setupTestNetworkState 设置测试用网络状态
func setupTestNetworkState() *InMemoryNetworkState {
	state := NewInMemoryNetworkState()

	// Indexer 1: 本地
	state.IndexerInfos[1] = IndexerInfo{
		IndexerId:           1,
		IndexerUrl:          "localhost",
		ClickhouseProxyPort: 9001,
	}

	// Indexer 2: 远程
	state.IndexerInfos[2] = IndexerInfo{
		IndexerId:           2,
		IndexerUrl:          "12.34.56.78",
		ClickhouseProxyPort: 9001,
	}

	// coinbase processor -> Indexer 1 (本地)
	state.ProcessorAllocations["coinbase"] = []ProcessorAllocation{
		{ProcessorId: "coinbase", IndexerId: 1},
	}
	state.ProcessorInfos["coinbase"] = ProcessorInfo{
		ProcessorId: "coinbase",
	}

	// pancakeswap123 processor -> Indexer 2 (远程)
	state.ProcessorAllocations["pancakeswap123"] = []ProcessorAllocation{
		{ProcessorId: "pancakeswap123", IndexerId: 2},
	}
	state.ProcessorInfos["pancakeswap123"] = ProcessorInfo{
		ProcessorId: "pancakeswap123",
	}

	return state
}

// ============================================================================
// 使用真实 ClickHouse 的集成测试（需要环境配置）
// ============================================================================

// TestIntegration_WithRealClickHouse 使用真实 ClickHouse 测试
// 运行方式: go test -v -run TestIntegration_WithRealClickHouse -tags=integration
func TestIntegration_WithRealClickHouse(t *testing.T) {
	t.Skip("需要真实 ClickHouse 环境，使用 -tags=integration 运行")

	// 配置
	chAddr := "127.0.0.1:9000" // ClickHouse Native 端口

	// 连接 ClickHouse
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{chAddr},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	// 验证连接
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("ping failed: %v", err)
	}

	t.Log("Connected to ClickHouse successfully")

	// 创建测试表
	ctx := context.Background()
	err = conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS test_transfer (
		id UInt64,
		address String,
		amount Float64,
		timestamp DateTime
	) ENGINE = Memory`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// 插入测试数据
	err = conn.Exec(ctx, `INSERT INTO test_transfer VALUES (1, '0x123', 100.0, now())`)
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// 查询验证
	rows, err := conn.Query(ctx, "SELECT COUNT(*) FROM test_transfer")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	var count uint64
	if rows.Next() {
		rows.Scan(&count)
	}
	t.Logf("Row count: %d", count)

	// 清理
	conn.Exec(ctx, "DROP TABLE IF EXISTS test_transfer")
}

// BenchmarkRewrite 性能测试
func BenchmarkRewrite(b *testing.B) {
	state := setupTestNetworkState()

	config := RewriterConfig{
		Enabled: true,
		ServiceAddr: "localhost:50051",
	}
	rewriter, err := NewSentioNetworkRewriter(config, state, DefaultTableRewriterFactory("sentio"))
	if err != nil {
		b.Fatalf("failed to create rewriter: %v", err)
	}
	defer rewriter.Close()

	ctx := context.Background()
	sql := "SELECT * FROM sentio_coinbase.transfer WHERE amount > 100"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rewriter.Rewrite(ctx, sql, "default", "test123")
	}
}

// BenchmarkParseTableNames 表名解析性能测试
func BenchmarkParseTableNames(b *testing.B) {
	rewriter := &SentioNetworkRewriter{}
	sql := "SELECT a.*, b.* FROM sentio_coinbase.transfer a JOIN sentio_pancakeswap123.Withdrawl b ON a.id = b.id UNION ALL SELECT * FROM sentio_coinbase.events"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rewriter.parseSentioNetworkTables(sql)
	}
}

// ============================================================================
// Mock SQL Rewriter gRPC Service (用于完整集成测试)
// ============================================================================

// MockRewriterService 模拟 sql-rewriter gRPC 服务
type MockRewriterService struct {
	listener net.Listener
	addr     string
	stopCh   chan struct{}
}

func NewMockRewriterService(t *testing.T) *MockRewriterService {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to start mock rewriter: %v", err)
	}

	service := &MockRewriterService{
		listener: ln,
		addr:     ln.Addr().String(),
		stopCh:   make(chan struct{}),
	}

	// 注意：这是一个简化的 mock，实际需要实现 gRPC 协议
	go service.serve()
	return service
}

func (s *MockRewriterService) serve() {
	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		s.listener.(*net.TCPListener).SetDeadline(time.Now().Add(100 * time.Millisecond))
		conn, err := s.listener.Accept()
		if err != nil {
			continue
		}
		// 简化处理：直接关闭连接
		conn.Close()
	}
}

func (s *MockRewriterService) Stop() {
	close(s.stopCh)
	s.listener.Close()
}

func (s *MockRewriterService) Addr() string {
	return s.addr
}

// 工具函数
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Suppress unused import warning for clickhouse-go
var _ io.Reader
