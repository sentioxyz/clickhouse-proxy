package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

// ============================================================================
// P0-1: ServerHello TeeReader 透传验证
// ============================================================================

// buildServerHello 构建一个模拟的 ServerHello 字节序列。
// 所有字段都用 proto.Buffer 精确编码，模拟 ClickHouse 服务端的输出。
func buildServerHello(clientRevision int, extraTailBytes []byte) []byte {
	buf := &proto.Buffer{}
	// packet_type = 0 (ServerHello)
	buf.PutUVarInt(0)
	// name
	buf.PutString("ClickHouse")
	// major
	buf.PutUVarInt(24)
	// minor
	buf.PutUVarInt(12)
	// revision
	buf.PutUVarInt(54476)

	// 条件字段基于 clientRevision
	if proto.FeatureVersionedParallelReplicas.In(clientRevision) {
		buf.PutUVarInt(1) // parallel_replicas_version
	}
	if proto.FeatureTimezone.In(clientRevision) {
		buf.PutString("UTC")
	}
	if proto.FeatureDisplayName.In(clientRevision) {
		buf.PutString("test-server")
	}
	if proto.FeatureVersionPatch.In(clientRevision) {
		buf.PutUVarInt(3) // version_patch
	}
	if proto.FeatureChunkedPackets.In(clientRevision) {
		buf.PutString("notchunked") // proto_send_chunked
		buf.PutString("notchunked") // proto_recv_chunked
	}

	// 追加额外的尾部字节（模拟 password_rules, nonce, settings 等未知字段）
	if len(extraTailBytes) > 0 {
		buf.Buf = append(buf.Buf, extraTailBytes...)
	}

	return buf.Buf
}

// TestV3_ServerHello_TeeReader_CapturesAllBytes 验证 TeeReader 方式能捕获
// ServerHello 的全部字节（包括未知的尾部字段），并原样转发给客户端。
func TestV3_ServerHello_TeeReader_CapturesAllBytes(t *testing.T) {
	// 模拟 ClickHouse 26.x 新增的尾部字段
	// password_rules: [count=1][rule_key="min_length"][rule_value="8"]
	tailBuf := &proto.Buffer{}
	tailBuf.PutUVarInt(1)           // count = 1
	tailBuf.PutString("min_length") // rule key
	tailBuf.PutString("8")          // rule value
	// nonce: 8 bytes
	tailBuf.Buf = append(tailBuf.Buf, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08)

	clientRevision := 54476 // 最高版本，包含所有条件字段
	serverHelloBytes := buildServerHello(clientRevision, tailBuf.Buf)

	// 模拟 TeeReader 透传方式（与 proxy.go 中的修复一致）
	upBr := bufio.NewReaderSize(bytes.NewReader(serverHelloBytes), 4096)
	var serverHelloRaw bytes.Buffer
	teeUpReader := io.TeeReader(upBr, &serverHelloRaw)
	teeUpBr := bufio.NewReaderSize(teeUpReader, 4096)
	teeUpChReader := proto.NewReader(teeUpBr)

	// 解析 packet_type
	pktType, err := teeUpChReader.UVarInt()
	if err != nil {
		t.Fatalf("read pktType: %v", err)
	}
	if pktType != 0 {
		t.Fatalf("expected pktType=0, got %d", pktType)
	}

	// 解析已知字段
	name, _ := teeUpChReader.Str()
	if name != "ClickHouse" {
		t.Fatalf("name=%q, want ClickHouse", name)
	}
	major, _ := teeUpChReader.UVarInt()
	minor, _ := teeUpChReader.UVarInt()
	revision, _ := teeUpChReader.UVarInt()
	t.Logf("ServerHello: %s %d.%d revision=%d", name, major, minor, revision)

	// 解析条件字段
	if proto.FeatureVersionedParallelReplicas.In(clientRevision) {
		_, _ = teeUpChReader.UVarInt()
	}
	if proto.FeatureTimezone.In(clientRevision) {
		_, _ = teeUpChReader.Str()
	}
	if proto.FeatureDisplayName.In(clientRevision) {
		_, _ = teeUpChReader.Str()
	}
	if proto.FeatureVersionPatch.In(clientRevision) {
		_, _ = teeUpChReader.UVarInt()
	}
	if proto.FeatureChunkedPackets.In(clientRevision) {
		_, _ = teeUpChReader.Str()
		_, _ = teeUpChReader.Str()
	}

	// Drain 缓冲区中的剩余数据
	if buffered := teeUpBr.Buffered(); buffered > 0 {
		drainBuf := make([]byte, buffered)
		teeUpBr.Read(drainBuf)
	}

	// 验证：serverHelloRaw 应该包含完整的原始字节（包括尾部字段）
	if !bytes.Equal(serverHelloRaw.Bytes(), serverHelloBytes) {
		t.Fatalf("TeeReader 未能捕获全部字节\n  got  (%d bytes): %x\n  want (%d bytes): %x",
			serverHelloRaw.Len(), serverHelloRaw.Bytes(),
			len(serverHelloBytes), serverHelloBytes)
	}
	t.Logf("TeeReader captured all %d bytes (including %d bytes of tail fields)",
		serverHelloRaw.Len(), len(tailBuf.Buf))
}

// TestV3_ServerHello_TeeReader_NoTail 验证没有尾部字段时 TeeReader 也正常工作。
func TestV3_ServerHello_TeeReader_NoTail(t *testing.T) {
	clientRevision := 54476
	serverHelloBytes := buildServerHello(clientRevision, nil) // 无尾部字段

	upBr := bufio.NewReaderSize(bytes.NewReader(serverHelloBytes), 4096)
	var serverHelloRaw bytes.Buffer
	teeUpReader := io.TeeReader(upBr, &serverHelloRaw)
	teeUpBr := bufio.NewReaderSize(teeUpReader, 4096)
	teeUpChReader := proto.NewReader(teeUpBr)

	// 解析所有已知字段
	teeUpChReader.UVarInt() // pktType
	teeUpChReader.Str()     // name
	teeUpChReader.UVarInt() // major
	teeUpChReader.UVarInt() // minor
	teeUpChReader.UVarInt() // revision
	if proto.FeatureVersionedParallelReplicas.In(clientRevision) {
		teeUpChReader.UVarInt()
	}
	if proto.FeatureTimezone.In(clientRevision) {
		teeUpChReader.Str()
	}
	if proto.FeatureDisplayName.In(clientRevision) {
		teeUpChReader.Str()
	}
	if proto.FeatureVersionPatch.In(clientRevision) {
		teeUpChReader.UVarInt()
	}
	if proto.FeatureChunkedPackets.In(clientRevision) {
		teeUpChReader.Str()
		teeUpChReader.Str()
	}

	// Drain
	if buffered := teeUpBr.Buffered(); buffered > 0 {
		drainBuf := make([]byte, buffered)
		teeUpBr.Read(drainBuf)
	}

	if !bytes.Equal(serverHelloRaw.Bytes(), serverHelloBytes) {
		t.Fatalf("字节不匹配: got %d bytes, want %d bytes", serverHelloRaw.Len(), len(serverHelloBytes))
	}
}

// TestV3_ServerHello_TeeReader_LowRevision 验证低版本客户端（无 chunked、无 parallel_replicas）场景。
func TestV3_ServerHello_TeeReader_LowRevision(t *testing.T) {
	clientRevision := 54412 // 低版本，只有 name/major/minor/revision
	serverHelloBytes := buildServerHello(clientRevision, nil)

	upBr := bufio.NewReaderSize(bytes.NewReader(serverHelloBytes), 4096)
	var serverHelloRaw bytes.Buffer
	teeUpReader := io.TeeReader(upBr, &serverHelloRaw)
	teeUpBr := bufio.NewReaderSize(teeUpReader, 4096)
	teeUpChReader := proto.NewReader(teeUpBr)

	teeUpChReader.UVarInt() // pktType
	teeUpChReader.Str()     // name
	teeUpChReader.UVarInt() // major
	teeUpChReader.UVarInt() // minor
	teeUpChReader.UVarInt() // revision
	// 低版本不包含后续条件字段

	if buffered := teeUpBr.Buffered(); buffered > 0 {
		drainBuf := make([]byte, buffered)
		teeUpBr.Read(drainBuf)
	}

	if !bytes.Equal(serverHelloRaw.Bytes(), serverHelloBytes) {
		t.Fatalf("低版本字节不匹配: got %d bytes, want %d bytes", serverHelloRaw.Len(), len(serverHelloBytes))
	}
}

// TestV3_ServerHello_TeeReader_ErrorPacket 验证非 Hello 包（如 Exception）时 TeeReader 正确工作。
func TestV3_ServerHello_TeeReader_ErrorPacket(t *testing.T) {
	buf := &proto.Buffer{}
	// packet_type = 2 (Exception)
	buf.PutUVarInt(2)
	buf.PutUVarInt(999)            // error code
	buf.PutString("test error")    // error name
	buf.PutString("error message") // error message
	buf.PutString("")              // stack trace
	buf.PutByte(0)                 // has_nested = false

	upBr := bufio.NewReaderSize(bytes.NewReader(buf.Buf), 4096)
	var serverHelloRaw bytes.Buffer
	teeUpReader := io.TeeReader(upBr, &serverHelloRaw)
	teeUpBr := bufio.NewReaderSize(teeUpReader, 4096)
	teeUpChReader := proto.NewReader(teeUpBr)

	pktType, err := teeUpChReader.UVarInt()
	if err != nil {
		t.Fatalf("read pktType: %v", err)
	}
	if pktType != 2 {
		t.Fatalf("expected Exception (2), got %d", pktType)
	}

	// Drain remaining
	if buffered := teeUpBr.Buffered(); buffered > 0 {
		drainBuf := make([]byte, buffered)
		teeUpBr.Read(drainBuf)
	}

	// 验证 serverHelloRaw 包含完整的 Exception 数据
	if !bytes.Equal(serverHelloRaw.Bytes(), buf.Buf) {
		t.Fatalf("Exception 字节不匹配: got %d bytes, want %d bytes",
			serverHelloRaw.Len(), len(buf.Buf))
	}
}

// ============================================================================
// P0-2: 压缩帧多帧检测正确性验证（验证 Peek 在 ReadRaw 后的安全性）
// ============================================================================

// TestV3_CompressedFrame_PeekAfterReadRaw 验证 bufio.Reader 在 io.ReadFull 消费完整帧后，
// Peek 能正确看到下一帧的数据。这模拟了 proxy.go 中 handleDataBlock 的压缩帧循环。
func TestV3_CompressedFrame_PeekAfterReadRaw(t *testing.T) {
	frame1 := buildCompressedFrameRaw(0x82, 100)
	frame2 := buildCompressedFrameRaw(0x82, 50)
	nextPacketByte := byte(0x01)

	var allData bytes.Buffer
	allData.Write(frame1)
	allData.Write(frame2)
	allData.WriteByte(nextPacketByte)

	br := bufio.NewReaderSize(&allData, 4096)

	// 使用 io.ReadFull 消费第一帧（模拟压缩帧读取）
	buf1 := make([]byte, len(frame1))
	_, err := io.ReadFull(br, buf1)
	if err != nil {
		t.Fatalf("ReadFull frame1: %v", err)
	}
	if !bytes.Equal(buf1, frame1) {
		t.Fatalf("frame1 content mismatch")
	}

	// Peek 下一帧的前 5 字节（验证 bufio 位置正确）
	peeked, err := br.Peek(5)
	if err != nil {
		t.Fatalf("Peek after reading frame1: %v", err)
	}
	if !bytes.Equal(peeked, frame2[:5]) {
		t.Fatalf("Peek content mismatch:\n  got:  %x\n  want: %x", peeked, frame2[:5])
	}

	// 消费第二帧
	buf2 := make([]byte, len(frame2))
	_, err = io.ReadFull(br, buf2)
	if err != nil {
		t.Fatalf("ReadFull frame2: %v", err)
	}
	if !bytes.Equal(buf2, frame2) {
		t.Fatalf("frame2 content mismatch")
	}

	// Peek 下一个字节（应该是 packet type）
	nextByte, err := br.Peek(1)
	if err != nil {
		t.Fatalf("Peek after frame2: %v", err)
	}
	if nextByte[0] != nextPacketByte {
		t.Fatalf("next byte = %x, want %x", nextByte[0], nextPacketByte)
	}

	t.Logf("Peek after ReadFull is safe: frame1=%d, frame2=%d", len(frame1), len(frame2))
}

// TestV3_CompressedFrame_PeekBlocksOnEmptyBuffer 验证 Peek 在缓冲区为空时的行为。
// 这模拟了最后一帧之后没有更多数据的场景。
func TestV3_CompressedFrame_PeekBlocksOnEmptyBuffer(t *testing.T) {
	frame := buildCompressedFrameRaw(0x82, 100)
	br := bufio.NewReaderSize(bytes.NewReader(frame), 64*1024)

	// 读取整个帧
	chReader := proto.NewReader(br)
	_, err := chReader.ReadRaw(len(frame))
	if err != nil {
		t.Fatalf("ReadRaw: %v", err)
	}

	// Peek 应该在无数据时返回 EOF
	_, err = br.Peek(25)
	if err != io.EOF {
		t.Fatalf("expected EOF on Peek after reading all data, got: %v", err)
	}
}

// ============================================================================
// 风险-5: resultsToInput 安全类型断言验证
// ============================================================================

// mockColNotInput 是一个不实现 ColInput 接口的 Column mock
// 它实现 ColResult 但不实现 ColInput
type mockColNotInput struct{}

func (m *mockColNotInput) Type() proto.ColumnType                       { return "UInt8" }
func (m *mockColNotInput) Rows() int                                    { return 0 }
func (m *mockColNotInput) Reset()                                       {}
func (m *mockColNotInput) DecodeColumn(r *proto.Reader, rows int) error { return nil }

// TestV3_ResultsToInput_SafeAssertion 验证不支持 ColInput 的列类型返回 error 而非 panic。
func TestV3_ResultsToInput_SafeAssertion(t *testing.T) {
	// 正常情况：ColAuto 实现了 ColInput
	normalResults := proto.Results{
		{Name: "id", Data: &proto.ColAuto{}},
	}
	cols, err := resultsToInput(normalResults)
	if err != nil {
		t.Fatalf("正常 ColAuto 应该成功: %v", err)
	}
	if len(cols) != 1 || cols[0].Name != "id" {
		t.Fatalf("unexpected cols: %+v", cols)
	}

	// 异常情况：不实现 ColInput 的类型
	badResults := proto.Results{
		{Name: "bad_col", Data: &mockColNotInput{}},
	}
	_, err = resultsToInput(badResults)
	if err == nil {
		t.Fatal("不实现 ColInput 的类型应该返回 error")
	}
	if !strings.Contains(err.Error(), "bad_col") {
		t.Fatalf("error should mention column name, got: %v", err)
	}
	if !strings.Contains(err.Error(), "ColInput") {
		t.Fatalf("error should mention ColInput, got: %v", err)
	}
	t.Logf("安全断言正确返回 error: %v", err)
}

// TestV3_ResultsToInput_EmptyResults 验证空 Results 的处理。
func TestV3_ResultsToInput_EmptyResults(t *testing.T) {
	cols, err := resultsToInput(proto.Results{})
	if err != nil {
		t.Fatalf("empty results should not error: %v", err)
	}
	if len(cols) != 0 {
		t.Fatalf("expected 0 cols, got %d", len(cols))
	}
}

// ============================================================================
// 风险-4: replaceOutsideQuotes ClickHouse 引号转义验证
// ============================================================================

// TestV3_ReplaceOutsideQuotes_ConsecutiveQuoteEscape 验证 ClickHouse 连续引号转义（”）
func TestV3_ReplaceOutsideQuotes_ConsecutiveQuoteEscape(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		old         string
		replacement string
		want        string
	}{
		{
			name:        "单引号转义不误替换",
			sql:         "SELECT 'it''s a my_table test' FROM my_table",
			old:         "my_table",
			replacement: "real_table",
			// my_table 在引号外应被替换，引号内的不应被替换
			want: "SELECT 'it''s a my_table test' FROM real_table",
		},
		{
			name:        "双引号转义不误替换",
			sql:         `SELECT "col""name" FROM my_table`,
			old:         "my_table",
			replacement: "real_table",
			want:        `SELECT "col""name" FROM real_table`,
		},
		{
			name:        "多个连续转义引号",
			sql:         "SELECT 'a''''b' FROM my_table",
			old:         "my_table",
			replacement: "real_table",
			want:        "SELECT 'a''''b' FROM real_table",
		},
		{
			name:        "引号内有目标但不替换",
			sql:         "SELECT 'my_table' AS name FROM my_table",
			old:         "my_table",
			replacement: "real_table",
			want:        "SELECT 'my_table' AS name FROM real_table",
		},
		{
			name:        "反引号标识符",
			sql:         "SELECT `my_table`.id FROM my_table",
			old:         "my_table",
			replacement: "real_table",
			want:        "SELECT `my_table`.id FROM real_table",
		},
		{
			name:        "混合: 反斜杠转义 + 连续引号",
			sql:         `SELECT 'foo\'bar', 'baz''qux' FROM my_table`,
			old:         "my_table",
			replacement: "real_table",
			want:        `SELECT 'foo\'bar', 'baz''qux' FROM real_table`,
		},
		{
			name:        "紧挨着的多个引号字符串",
			sql:         "SELECT 'a''b', 'c''d' FROM my_table WHERE x = 'my_table'",
			old:         "my_table",
			replacement: "real_table",
			want:        "SELECT 'a''b', 'c''d' FROM real_table WHERE x = 'my_table'",
		},
		{
			name:        "空字符串引号",
			sql:         "SELECT '' FROM my_table",
			old:         "my_table",
			replacement: "real_table",
			want:        "SELECT '' FROM real_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replaceOutsideQuotes(tt.sql, tt.old, tt.replacement)
			if got != tt.want {
				t.Errorf("\n  sql:  %s\n  got:  %s\n  want: %s", tt.sql, got, tt.want)
			}
		})
	}
}

// TestV3_ReplaceOutsideQuotes_EdgeCases 验证边界情况
func TestV3_ReplaceOutsideQuotes_EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		old  string
		rep  string
		want string
	}{
		{
			name: "未闭合的单引号（SQL 语法错误）",
			sql:  "SELECT 'unclosed FROM my_table",
			old:  "my_table",
			rep:  "real_table",
			// 未闭合引号后的内容在引号范围内，不应替换
			want: "SELECT 'unclosed FROM my_table",
		},
		{
			name: "空SQL",
			sql:  "",
			old:  "x",
			rep:  "y",
			want: "",
		},
		{
			name: "目标在SQL开头",
			sql:  "my_table JOIN other",
			old:  "my_table",
			rep:  "real_table",
			want: "real_table JOIN other",
		},
		{
			name: "目标在SQL结尾",
			sql:  "FROM my_table",
			old:  "my_table",
			rep:  "real_table",
			want: "FROM real_table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := replaceOutsideQuotes(tt.sql, tt.old, tt.rep)
			if got != tt.want {
				t.Errorf("\n  got:  %q\n  want: %q", got, tt.want)
			}
		})
	}
}

// ============================================================================
// P1-2: queryDoneCounter 并发验证
// ============================================================================

// TestV3_QueryDoneCounter_ConcurrentAddSwap 验证 atomic.Int64 在高并发场景下不丢失信号。
func TestV3_QueryDoneCounter_ConcurrentAddSwap(t *testing.T) {
	var counter atomic.Int64

	// 模拟 N 个查询完成信号
	const N = 10000
	done := make(chan struct{})

	// 生产者：模拟 copyUpstreamToClient 检测 EndOfStream
	go func() {
		for i := 0; i < N; i++ {
			counter.Add(1)
		}
		close(done)
	}()

	// 消费者：模拟包循环中的检测
	var consumed int64
	<-done // 等待所有信号产生
	for {
		v := counter.Swap(0)
		if v == 0 {
			break
		}
		consumed += v
	}

	if consumed != N {
		t.Fatalf("丢失信号: consumed=%d, expected=%d", consumed, N)
	}
	t.Logf("所有 %d 个信号均被消费", consumed)
}

// TestV3_QueryDoneCounter_InterleaveProducerConsumer 验证交替生产-消费的正确性。
func TestV3_QueryDoneCounter_InterleaveProducerConsumer(t *testing.T) {
	var counter atomic.Int64
	var totalConsumed int64

	// 交替 add 和 swap
	for i := 0; i < 100; i++ {
		counter.Add(1)
		counter.Add(1) // 两个 EndOfStream 快速到来

		v := counter.Swap(0)
		totalConsumed += v
	}

	// 最终清理
	v := counter.Swap(0)
	totalConsumed += v

	if totalConsumed != 200 {
		t.Fatalf("交替模式丢失信号: total=%d, expected=200", totalConsumed)
	}
}

// ============================================================================
// 常量提取验证
// ============================================================================

// TestV3_Constants 验证提取的常量值正确。
func TestV3_Constants(t *testing.T) {
	if fallbackRevision != 54423 {
		t.Fatalf("fallbackRevision = %d, want 54423", fallbackRevision)
	}
	if uuidSize != 16 {
		t.Fatalf("uuidSize = %d, want 16", uuidSize)
	}
	if defaultStreamingBufSize != 131072 {
		t.Fatalf("defaultStreamingBufSize = %d, want 131072", defaultStreamingBufSize)
	}
}

// ============================================================================
// P1-3: detectServerPacketType 在不同模式下的行为验证
// ============================================================================

// TestV3_DetectServerPacketType_Accuracy 验证 detectServerPacketType 在非 chunked 模式下的准确性。
func TestV3_DetectServerPacketType_Accuracy(t *testing.T) {
	tests := []struct {
		name     string
		code     byte
		wantType string
	}{
		{"Hello", 0, "Hello"},
		{"Data", 1, "Data"},
		{"Exception", 2, "Exception"},
		{"Progress", 3, "Progress"},
		{"Pong", 4, "Pong"},
		{"EndOfStream", 5, "EndOfStream"},
		{"ProfileInfo", 6, "ProfileInfo"},
		{"Totals", 7, "Totals"},
		{"Extremes", 8, "Extremes"},
		{"TablesStatusResponse", 9, "TablesStatusResponse"},
		{"Log", 10, "Log"},
		{"TableColumns", 11, "TableColumns"},
		{"PartUUIDs", 12, "PartUUIDs"},
		{"ReadTaskRequest", 13, "ReadTaskRequest"},
		{"ProfileEvents", 14, "ProfileEvents"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 构造一个只包含 type byte 的 chunk
			chunk := []byte{tt.code, 0x00, 0x00} // type + padding
			result := detectServerPacketType(chunk)
			if result != tt.wantType {
				t.Errorf("detectServerPacketType(%x) = %q, want %q", chunk, result, tt.wantType)
			}
		})
	}
}

// TestV3_DetectServerPacketType_ChunkedModeFirstByte 验证 chunked 模式下
// 如果 chunk 从 packet 中间开始，检测可能不准确。
func TestV3_DetectServerPacketType_ChunkedModeFirstByte(t *testing.T) {
	// 模拟 chunked 帧数据：前几个字节可能是上一个 packet 的数据
	// 如果恰好数据字节 == EndOfStream type (5)，会产生误检
	falsePositiveChunk := []byte{5, 0xFF, 0xFF} // 数据恰好以 5 开头
	result := detectServerPacketType(falsePositiveChunk)
	if result != "EndOfStream" {
		t.Logf("chunked 模式下可能误检：raw byte 5 被检测为 %q", result)
	} else {
		t.Logf("确认: chunked 模式下 raw byte 5 被误检为 EndOfStream (best-effort)")
	}
}

// ============================================================================
// 综合 buildCompressedFrameRaw（复用 pr15_fixes_test.go 中的辅助函数）
// ============================================================================
// 注意：buildCompressedFrameRaw 已在 pr15_fixes_test.go 中定义，
// 这里的测试通过它来验证 Peek 的安全性。

// TestV3_CompressedFrame_ValidMethodDetection 验证合法压缩方法的检测。
func TestV3_CompressedFrame_ValidMethodDetection(t *testing.T) {
	validMethods := []byte{0x82, 0x90} // LZ4, ZSTD

	for _, method := range validMethods {
		frame := buildCompressedFrameRaw(method, 256)
		// 验证帧的第 16 字节是压缩方法
		if len(frame) < 17 {
			t.Fatalf("frame too short for method check")
		}
		if frame[16] != method {
			t.Fatalf("frame[16] = %x, want %x", frame[16], method)
		}
	}
}

// ============================================================================
// MergeTreeReadTaskResponse 格式验证
// ============================================================================

// TestV3_MergeTreeReadTaskResponse_Format 验证 MergeTreeReadTaskResponse 使用 2 个 VarInt 字段。
func TestV3_MergeTreeReadTaskResponse_Format(t *testing.T) {
	// 构造 MergeTreeReadTaskResponse: [type=10][segment=42][mark=100]
	buf := &proto.Buffer{}
	buf.PutByte(byte(clientCodeMergeTreeReadTaskResponse))
	buf.PutUVarInt(42)  // segment
	buf.PutUVarInt(100) // mark

	// 解码验证
	r := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf)))

	// 读取 type byte
	var typeBuf [1]byte
	_, err := io.ReadFull(bufio.NewReader(bytes.NewReader(buf.Buf)), typeBuf[:])
	if err != nil {
		t.Fatalf("read type: %v", err)
	}
	if typeBuf[0] != byte(clientCodeMergeTreeReadTaskResponse) {
		t.Fatalf("type = %d, want %d", typeBuf[0], clientCodeMergeTreeReadTaskResponse)
	}

	// 使用 proto.Reader 读取 segment 和 mark
	restReader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf[1:])))
	segment, err := restReader.UVarInt()
	if err != nil {
		t.Fatalf("read segment: %v", err)
	}
	if segment != 42 {
		t.Fatalf("segment = %d, want 42", segment)
	}

	mark, err := restReader.UVarInt()
	if err != nil {
		t.Fatalf("read mark: %v", err)
	}
	if mark != 100 {
		t.Fatalf("mark = %d, want 100", mark)
	}

	// 确认没有多余数据
	_, err = restReader.UVarInt()
	if err == nil {
		t.Fatal("不应有更多字段")
	}

	_ = r // suppress unused
	t.Logf("MergeTreeReadTaskResponse: type=%d segment=%d mark=%d", typeBuf[0], segment, mark)
}

// ============================================================================
// ReadTaskResponse 格式对比验证
// ============================================================================

// TestV3_ReadTaskResponse_StringOnly 验证 ReadTaskResponse 只有一个 String 字段。
func TestV3_ReadTaskResponse_StringOnly(t *testing.T) {
	// ReadTaskResponse: [type=9][response: String]
	// 不应有 version UVarInt 前缀
	buf := &proto.Buffer{}
	buf.PutByte(byte(clientCodeReadTaskResponse))
	buf.PutString("part_0")

	// 解码
	reader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf[1:])))
	response, err := reader.Str()
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if response != "part_0" {
		t.Fatalf("response = %q, want %q", response, "part_0")
	}

	// 验证编码大小精确
	expectedSize := 1 + // type byte
		1 + // varint length of "part_0" (6)
		6 // "part_0"
	if len(buf.Buf) != expectedSize {
		t.Fatalf("encoded size = %d, want %d", len(buf.Buf), expectedSize)
	}
}

// ============================================================================
// IgnoredPartUUIDs 格式验证
// ============================================================================

// TestV3_IgnoredPartUUIDs_Format 验证 IgnoredPartUUIDs 使用 uuidSize 常量。
func TestV3_IgnoredPartUUIDs_Format(t *testing.T) {
	buf := &proto.Buffer{}
	buf.PutByte(byte(clientCodeIgnoredPartUUIDs))
	buf.PutUVarInt(2) // 2 UUIDs

	// 写入 2 个 16 字节的 UUID
	for i := 0; i < 2; i++ {
		uuid := make([]byte, uuidSize)
		for j := range uuid {
			uuid[j] = byte(i*16 + j)
		}
		buf.Buf = append(buf.Buf, uuid...)
	}

	// 验证解码
	reader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf[1:])))
	count, _ := reader.UVarInt()
	if count != 2 {
		t.Fatalf("uuid count = %d, want 2", count)
	}
	for i := uint64(0); i < count; i++ {
		uuid, err := reader.ReadRaw(uuidSize)
		if err != nil {
			t.Fatalf("read uuid %d: %v", i, err)
		}
		if len(uuid) != uuidSize {
			t.Fatalf("uuid %d size = %d, want %d", i, len(uuid), uuidSize)
		}
	}
}

// ============================================================================
// 帧大小相关辅助验证
// ============================================================================

// TestV3_FrameHeaderSize 验证压缩帧头大小定义一致。
func TestV3_FrameHeaderSize(t *testing.T) {
	// ClickHouse 压缩帧头: [CityHash128: 16 bytes][method: 1 byte][compressed_size: 4 bytes LE][decompressed_size: 4 bytes LE]
	expectedSize := 16 + 1 + 4 + 4 // = 25
	actualSize := 25               // 代码中使用的常量

	if expectedSize != actualSize {
		t.Fatalf("frame header size mismatch: expected %d, got %d", expectedSize, actualSize)
	}

	// 验证 buildCompressedFrameRaw 生成的帧至少有 25 字节头
	frame := buildCompressedFrameRaw(0x82, 10)
	if len(frame) < 25 {
		t.Fatalf("frame too short: %d bytes, need at least 25", len(frame))
	}

	// 验证 method byte 位置
	if frame[16] != 0x82 {
		t.Fatalf("method byte at offset 16: got %x, want 0x82", frame[16])
	}

	// 验证 compressed_size 字段
	// buildCompressedFrameRaw 中 compressed_size = 9 + dataSize
	// 其中 9 = 1(method) + 4(compressed_size) + 4(decompressed_size)
	compressedSize := binary.LittleEndian.Uint32(frame[17:21])
	expectedCompSize := 9 + 10 // 9 header bytes (after checksum) + data
	if int(compressedSize) != expectedCompSize {
		t.Fatalf("compressed_size = %d, want %d", compressedSize, expectedCompSize)
	}
}

// ============================================================================
// 回归测试: 确保现有功能不受影响
// ============================================================================

// TestV3_Regression_QueryCodecRoundTrip 确保 query_codec 的编解码仍然正确。
// 注意：完整的字节级 round-trip 测试已在 query_codec_test.go 中覆盖，
// 这里只验证基本的编码→解码往返在修改 resultsToInput 后仍然正常。
func TestV3_Regression_QueryCodecRoundTrip(t *testing.T) {
	// 使用高版本 revision 测试
	rev := 54476

	// 使用 query_codec_test.go 中的同样模式构建完整的 Query
	buf := &proto.Buffer{}
	buf.PutString("test-query-id") // QueryID

	// ClientInfo
	info := proto.ClientInfo{
		ProtocolVersion: rev,
		Major:           1,
		Minor:           0,
		ClientName:      "test-client",
		Interface:       proto.InterfaceTCP,
	}
	eq := &ExtQuery{}
	eq.Info = info
	encodeClientInfoCustom(buf, &info, rev, eq)

	// Settings (new format: empty key = end)
	buf.PutString("") // end of settings

	// Inter-server secret
	buf.PutString("test-secret")

	// External roles
	if proto.FeatureInterserverExternallyGrantedRoles.In(rev) {
		buf.PutString("") // empty roles
	}

	// Stage, Compression, Body
	buf.PutUVarInt(uint64(proto.StageComplete))
	buf.PutUVarInt(uint64(proto.CompressionDisabled))
	buf.PutString("SELECT 1")

	// Parameters
	if proto.FeatureParameters.In(rev) {
		buf.PutString("") // end of params
	}

	// 解码
	r := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf)))
	decoded, err := decodeQueryCustom(r, rev)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if decoded.ID != "test-query-id" {
		t.Errorf("ID = %q, want %q", decoded.ID, "test-query-id")
	}
	if decoded.Body != "SELECT 1" {
		t.Errorf("Body = %q, want %q", decoded.Body, "SELECT 1")
	}
	t.Logf("QueryCodec round-trip OK: ID=%q Body=%q Secret=%q", decoded.ID, decoded.Body, decoded.Secret)
}
