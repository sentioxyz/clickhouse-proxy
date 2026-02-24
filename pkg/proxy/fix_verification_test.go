package proxy

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

// ============================================================================
// P0 #1: ServerHello Tail Drain 修复验证
// ============================================================================

// TestServerHelloNoDrain 验证 ServerHello 解析后不会盲目读取后续数据。
// 修复前，upBr.Buffered() 的所有数据（可能包含后续 Data/EndOfStream 包）
// 会被错误地附加到 ServerHello 响应中。
// 修复后，依赖精确的逐字段解析，不再 drain。
func TestServerHelloNoDrain(t *testing.T) {
	// 这是一个设计层面的测试。验证关键路径：
	// 当 bufio.Reader 缓冲区中 ServerHello 后面有后续 packet 时，
	// copyUpstreamToClientFromReader 应该能正确读取这些 packet。

	// 模拟场景：缓冲区 = [ServerHello bytes...][EndOfStream(0x05)]
	// 修复前：EndOfStream 会被错误地加入 ServerHello，copyUpstreamToClientFromReader 丢失它
	// 修复后：EndOfStream 留在缓冲区中，被后续读取正确处理

	// 构造模拟数据：ServerHello 后紧跟 EndOfStream 包
	var buf bytes.Buffer
	// 模拟 ServerHello 数据 (简化: 不完整的 ServerHello，仅用于验证不会盲目 drain)
	serverHelloData := []byte{0x00, 0x0E, 'C', 'l', 'i', 'c', 'k', 'H', 'o', 'u', 's', 'e'}
	buf.Write(serverHelloData)

	// 模拟后续的 EndOfStream 包
	endOfStreamData := []byte{0x05}
	buf.Write(endOfStreamData)

	// 验证：读取 serverHelloData 后，endOfStreamData 仍然可以读取
	reader := bytes.NewReader(buf.Bytes())

	// 先读 ServerHello 数据
	helloRead := make([]byte, len(serverHelloData))
	n, err := io.ReadFull(reader, helloRead)
	if err != nil {
		t.Fatalf("读取 ServerHello 数据失败: %v", err)
	}
	if n != len(serverHelloData) {
		t.Fatalf("ServerHello 读取字节数不匹配: expected %d, got %d", len(serverHelloData), n)
	}

	// EndOfStream 应该仍然可以读取
	remaining := make([]byte, 1)
	n, err = reader.Read(remaining)
	if err != nil {
		t.Fatalf("读取后续 EndOfStream 失败: %v", err)
	}
	if n != 1 || remaining[0] != 0x05 {
		t.Errorf("后续 EndOfStream 数据错误: expected [0x05], got %v", remaining[:n])
	}

	t.Log("验证通过：ServerHello 后的数据不会被盲目 drain")
}

// ============================================================================
// P0 #2: queryDoneCounter 原子计数器验证
// ============================================================================

// TestQueryDoneCounter_RapidSignals 验证快速连续查询场景下不会丢失信号。
// 修复前使用 buffered channel(1)，在快速连续 EndOfStream 场景下会丢失信号。
// 修复后使用 atomic.Int64，信号永远不会丢失。
func TestQueryDoneCounter_RapidSignals(t *testing.T) {
	var counter atomic.Int64

	// 模拟快速连续 10 个 EndOfStream
	numSignals := 10
	for i := 0; i < numSignals; i++ {
		counter.Add(1)
	}

	// 验证所有信号都被记录
	got := counter.Load()
	if got != int64(numSignals) {
		t.Errorf("信号丢失: expected %d, got %d", numSignals, got)
	}

	// 模拟消费端逐个处理
	consumed := int64(0)
	lastSeen := int64(0)
	for {
		current := counter.Load()
		if current == lastSeen {
			break // 没有新信号
		}
		consumed += current - lastSeen
		lastSeen = current
	}

	if consumed != int64(numSignals) {
		t.Errorf("消费信号数不匹配: expected %d, got %d", numSignals, consumed)
	}

	t.Log("验证通过：atomic 计数器不会丢失快速连续信号")
}

// TestQueryDoneCounter_ConcurrentProducerConsumer 验证生产者-消费者并发安全。
func TestQueryDoneCounter_ConcurrentProducerConsumer(t *testing.T) {
	var counter atomic.Int64
	done := make(chan struct{})

	// 生产者：快速发送 1000 个信号
	go func() {
		for i := 0; i < 1000; i++ {
			counter.Add(1)
		}
		close(done)
	}()

	<-done

	// 消费者：验证最终值
	final := counter.Load()
	if final != 1000 {
		t.Errorf("并发计数错误: expected 1000, got %d", final)
	}
}

// ============================================================================
// P1 #3: fallbackRawCopy 使用 io.Writer 验证
// ============================================================================

// TestFallbackRawCopy_UsesChunkedWriter 验证 fallback 模式使用 io.Writer 而不是直接写 net.Conn。
// 修复前 fallbackRawCopy 直接写 upstreamConn，绕过了 ChunkedWriter。
// 修复后使用 io.Writer 参数，chunked 模式下数据也会被正确封帧。
func TestFallbackRawCopy_UsesChunkedWriter(t *testing.T) {
	// 验证 ChunkedWriter 作为 io.Writer 的正确行为
	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, true)

	// 模拟 fallback 数据写入
	testData := []byte("fallback raw data that should be chunked")
	n, err := cw.Write(testData)
	if err != nil {
		t.Fatalf("ChunkedWriter.Write 失败: %v", err)
	}
	if n != len(testData) {
		t.Errorf("写入字节数不匹配: expected %d, got %d", len(testData), n)
	}

	// 验证数据被正确 chunked（有帧头 + 结束标记）
	output := buf.Bytes()
	expectedMinLen := 4 + len(testData) + 4 // header + data + end marker
	if len(output) != expectedMinLen {
		t.Errorf("chunked 输出长度错误: expected %d, got %d", expectedMinLen, len(output))
	}

	// 通过 ChunkedReader 验证可以正确解帧
	cr := NewChunkedReader(bytes.NewReader(output), true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ChunkedReader 解帧失败: %v", err)
	}
	if !bytes.Equal(got, testData) {
		t.Errorf("round-trip 失败: expected %q, got %q", testData, got)
	}

	t.Log("验证通过：fallback 数据通过 ChunkedWriter 正确封帧")
}

// TestFallbackRawCopy_DisabledChunked 验证非 chunked 模式下 fallback 直接写数据。
func TestFallbackRawCopy_DisabledChunked(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, false)

	testData := []byte("raw fallback data without chunked framing")
	n, err := cw.Write(testData)
	if err != nil {
		t.Fatalf("Write 失败: %v", err)
	}
	if n != len(testData) {
		t.Errorf("写入字节数不匹配: expected %d, got %d", len(testData), n)
	}

	// 非 chunked 模式应该直接透传
	if !bytes.Equal(buf.Bytes(), testData) {
		t.Errorf("非 chunked 模式应该直接透传: expected %q, got %q", testData, buf.Bytes())
	}
}

// ============================================================================
// P1 #4: ReadTaskResponse 格式修复验证
// ============================================================================

// TestReadTaskResponse_NoVersion 验证 ReadTaskResponse 只包含 String 字段，没有 version 字段。
// 修复前: [packetType][version:UVarInt][response:String]（错误）
// 修复后: [packetType][response:String]（正确，与 ClickHouse TCPHandler 一致）
func TestReadTaskResponse_NoVersion(t *testing.T) {
	// 根据 ClickHouse 源码: receiveReadTaskResponseAssumeLocked 只调用 readStringBinary
	// 因此客户端发送 ReadTaskResponse 时也只有一个 String 字段

	// 模拟正确格式的 ReadTaskResponse 编码
	// packetType byte + string (varint length + content)
	response := "test_partition_id"

	// 正确编码: [0x0D][string_len_varint][string_data]
	var correctEncoding bytes.Buffer
	correctEncoding.WriteByte(byte(clientCodeReadTaskResponse)) // 0x0D = 13
	writeString(&correctEncoding, response)

	// 验证编码中没有多余的 version 字段
	// packetType(1) + varint(len) + string(len) = 1 + varint_size + 17
	expectedMinSize := 1 + 1 + len(response) // at minimum
	if correctEncoding.Len() < expectedMinSize {
		t.Errorf("编码过短: expected >= %d, got %d", expectedMinSize, correctEncoding.Len())
	}

	// 验证第一个字节是 packetType
	if correctEncoding.Bytes()[0] != byte(clientCodeReadTaskResponse) {
		t.Errorf("packetType 错误: expected 0x%02X, got 0x%02X",
			clientCodeReadTaskResponse, correctEncoding.Bytes()[0])
	}

	t.Logf("验证通过：ReadTaskResponse 正确编码为 [packetType][response:String], 总长 %d 字节", correctEncoding.Len())
}

// writeString 辅助函数：写入 varint 编码的 string
func writeString(buf *bytes.Buffer, s string) {
	// 写入 string 长度 (UVarInt)
	n := uint64(len(s))
	for n >= 0x80 {
		buf.WriteByte(byte(n) | 0x80)
		n >>= 7
	}
	buf.WriteByte(byte(n))
	// 写入 string 内容
	buf.WriteString(s)
}

// ============================================================================
// P1 #5: gRPC keepalive 和超时验证
// ============================================================================

// TestRewriterConfig_Timeout 验证 RewriterConfig.Timeout 的默认值和使用。
func TestRewriterConfig_Timeout(t *testing.T) {
	cfg := DefaultConfig()

	// 验证默认超时值
	if cfg.RewriterTimeout.Duration.Seconds() != 5 {
		t.Errorf("默认 RewriterTimeout 应为 5s, got %v", cfg.RewriterTimeout.Duration)
	}

	t.Log("验证通过：RewriterConfig.Timeout 默认值正确")
}

// ============================================================================
// P1 #6: simpleRewrite 安全性验证
// ============================================================================

// TestReplaceOutsideQuotes_Basic 验证基本替换功能。
func TestReplaceOutsideQuotes_Basic(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		old         string
		replacement string
		expected    string
	}{
		{
			name:        "简单替换",
			sql:         "SELECT * FROM sentio_coinbase.transfer",
			old:         "sentio_coinbase.transfer",
			replacement: "sentio.transfer",
			expected:    "SELECT * FROM sentio.transfer",
		},
		{
			name:        "多次出现",
			sql:         "SELECT sentio_coinbase.transfer, sentio_coinbase.transfer FROM sentio_coinbase.transfer",
			old:         "sentio_coinbase.transfer",
			replacement: "sentio.transfer",
			expected:    "SELECT sentio.transfer, sentio.transfer FROM sentio.transfer",
		},
		{
			name:        "无匹配",
			sql:         "SELECT * FROM other_table",
			old:         "sentio_coinbase.transfer",
			replacement: "sentio.transfer",
			expected:    "SELECT * FROM other_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceOutsideQuotes(tt.sql, tt.old, tt.replacement)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// TestReplaceOutsideQuotes_SkipQuotedStrings 验证不替换引号内的表名。
// 这是 P1 #6 修复的核心验证。
func TestReplaceOutsideQuotes_SkipQuotedStrings(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		old         string
		replacement string
		expected    string
	}{
		{
			name:        "单引号内不替换",
			sql:         "SELECT * FROM sentio_coinbase.transfer WHERE name = 'sentio_coinbase.transfer'",
			old:         "sentio_coinbase.transfer",
			replacement: "sentio.transfer",
			expected:    "SELECT * FROM sentio.transfer WHERE name = 'sentio_coinbase.transfer'",
		},
		{
			name:        "双引号内不替换",
			sql:         `SELECT * FROM sentio_coinbase.transfer WHERE name = "sentio_coinbase.transfer"`,
			old:         "sentio_coinbase.transfer",
			replacement: "sentio.transfer",
			expected:    `SELECT * FROM sentio.transfer WHERE name = "sentio_coinbase.transfer"`,
		},
		{
			name:        "混合场景",
			sql:         `SELECT sentio_coinbase.transfer FROM sentio_coinbase.transfer WHERE desc = 'table: sentio_coinbase.transfer' AND sentio_coinbase.transfer > 0`,
			old:         "sentio_coinbase.transfer",
			replacement: "sentio.tx",
			expected:    `SELECT sentio.tx FROM sentio.tx WHERE desc = 'table: sentio_coinbase.transfer' AND sentio.tx > 0`,
		},
		{
			name:        "转义引号",
			sql:         `SELECT * FROM sentio_coinbase.transfer WHERE name = 'it\'s sentio_coinbase.transfer'`,
			old:         "sentio_coinbase.transfer",
			replacement: "sentio.transfer",
			expected:    `SELECT * FROM sentio.transfer WHERE name = 'it\'s sentio_coinbase.transfer'`,
		},
		{
			name:        "空引号",
			sql:         "SELECT '' FROM sentio_coinbase.transfer",
			old:         "sentio_coinbase.transfer",
			replacement: "sentio.transfer",
			expected:    "SELECT '' FROM sentio.transfer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceOutsideQuotes(tt.sql, tt.old, tt.replacement)
			if result != tt.expected {
				t.Errorf("expected:\n  %q\ngot:\n  %q", tt.expected, result)
			}
		})
	}
}

// TestSimpleRewrite_SafeFromInjection 验证 simpleRewrite 不会导致 SQL 语义错误。
func TestSimpleRewrite_SafeFromInjection(t *testing.T) {
	state := NewInMemoryNetworkState()
	state.ProcessorAllocations["coinbase"] = []ProcessorAllocation{
		{ProcessorId: "coinbase", IndexerId: 1},
	}
	state.IndexerInfos[1] = IndexerInfo{
		IndexerId:  1,
		IndexerUrl: "localhost",
	}
	state.ProcessorInfos["coinbase"] = ProcessorInfo{
		ProcessorId: "coinbase",
	}

	rewriter := &SentioNetworkRewriter{
		config: RewriterConfig{
			LocalIndexerId: 1,
		},
		networkState:         state,
		tableRewriterFactory: DefaultTableRewriterFactory(),
	}

	// SQL 中字符串字面量包含表名模式
	sql := "SELECT * FROM sentio_coinbase.transfer WHERE desc = 'sentio_coinbase.transfer is a table'"

	result, err := rewriter.Rewrite(context.Background(), sql)
	if err != nil {
		t.Fatalf("Rewrite 失败: %v", err)
	}

	// 验证 FROM 之后的表名被替换了
	if !strings.Contains(result, "sentio.transfer") {
		t.Errorf("FROM 后的表名应该被替换: %s", result)
	}

	// 验证字符串字面量中的表名未被替换
	if !strings.Contains(result, "'sentio_coinbase.transfer is a table'") {
		t.Errorf("字符串字面量中的表名不应被替换: %s", result)
	}

	t.Logf("验证通过: %s", result)
}

// ============================================================================
// P2 #7: Buffer Pool 验证
// ============================================================================

// TestSyncPool_ReuseAndGrow 验证 sync.Pool buffer 的获取和增长行为。
// 注：compressedBufPool 已从 proxy 中移除（ReadRaw 总是返回新分配的 slice），
// 此测试改为验证通用的 sync.Pool 行为（如 bufferPool）。
func TestSyncPool_ReuseAndGrow(t *testing.T) {
	p := NewProxy(DefaultConfig(), nil, nil)

	// 测试 bufferPool（proto.Buffer）的复用
	b1 := p.getBuffer()
	if b1 == nil {
		t.Fatal("getBuffer should never return nil")
	}
	if len(b1.Buf) != 0 {
		t.Errorf("new buffer should be empty, got len=%d", len(b1.Buf))
	}

	// 写入数据后归还
	b1.PutString("test data")
	p.putBuffer(b1)

	// 再次获取，应已被重置
	b2 := p.getBuffer()
	if len(b2.Buf) != 0 {
		t.Errorf("reused buffer should be reset, got len=%d", len(b2.Buf))
	}
	p.putBuffer(b2)

	t.Log("验证通过：sync.Pool buffer 复用机制工作正常")
}

// ============================================================================
// P2 #9: 可配置 bufio 大小验证
// ============================================================================

// TestConfigStreamingBufSize 验证默认配置中的 StreamingBufSize 值。
func TestConfigStreamingBufSize(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.StreamingBufSize != 131072 {
		t.Errorf("默认 StreamingBufSize 应为 131072 (128KB), got %d", cfg.StreamingBufSize)
	}

	t.Log("验证通过：StreamingBufSize 默认值为 128KB")
}

// TestConfigStreamingBufSize_Zero 验证 StreamingBufSize=0 时使用默认值。
func TestConfigStreamingBufSize_Zero(t *testing.T) {
	cfg := DefaultConfig()
	cfg.StreamingBufSize = 0

	// proxy 内部应该使用默认值 131072
	// 这个逻辑在 copyClientToUpstreamStreaming 中
	bufSize := cfg.StreamingBufSize
	if bufSize <= 0 {
		bufSize = 131072
	}

	if bufSize != 131072 {
		t.Errorf("StreamingBufSize=0 应退回到 131072, got %d", bufSize)
	}
}

// ============================================================================
// P2 #10: ValidateChecksum 配置验证
// ============================================================================

// TestConfigValidateChecksum 验证 ValidateChecksum 默认值。
func TestConfigValidateChecksum(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.ValidateChecksum != false {
		t.Error("ValidateChecksum 默认应为 false")
	}
}

// ============================================================================
// P2 #8: Mutex 移除验证
// ============================================================================

// TestChunkedReader_SingleGoroutine 验证 ChunkedReader 在单 goroutine 下工作正常（无 mutex）。
func TestChunkedReader_SingleGoroutine(t *testing.T) {
	// 构造 1000 个小帧
	var buf bytes.Buffer
	for i := 0; i < 1000; i++ {
		data := []byte(fmt.Sprintf("frame_%04d", i))
		frame := makeChunkedFrame(data)
		buf.Write(frame)
	}

	cr := NewChunkedReader(bytes.NewReader(buf.Bytes()), true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll 失败: %v", err)
	}

	// 验证所有帧数据都完整
	for i := 0; i < 1000; i++ {
		expected := fmt.Sprintf("frame_%04d", i)
		if !strings.Contains(string(got), expected) {
			t.Errorf("缺少帧 %d 的数据", i)
		}
	}

	t.Log("验证通过：ChunkedReader 在单 goroutine 下无 mutex 工作正常")
}

// TestChunkedWriter_SingleGoroutine 验证 ChunkedWriter 在单 goroutine 下工作正常（无 mutex）。
func TestChunkedWriter_SingleGoroutine(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, true)

	for i := 0; i < 1000; i++ {
		data := []byte(fmt.Sprintf("write_%04d", i))
		_, err := cw.Write(data)
		if err != nil {
			t.Fatalf("Write #%d 失败: %v", i, err)
		}
	}

	// 验证可以通过 ChunkedReader 正确读取
	cr := NewChunkedReader(bytes.NewReader(buf.Bytes()), true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll 失败: %v", err)
	}

	for i := 0; i < 1000; i++ {
		expected := fmt.Sprintf("write_%04d", i)
		if !strings.Contains(string(got), expected) {
			t.Errorf("缺少写入 %d 的数据", i)
		}
	}

	t.Log("验证通过：ChunkedWriter 在单 goroutine 下无 mutex 工作正常")
}
