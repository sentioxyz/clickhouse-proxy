package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"testing"
)

// ============================================================================
// ChunkedReader 单元测试
// ============================================================================

// TestChunkedReader_BasicChunk 测试单帧读取（最基本的场景）
func TestChunkedReader_BasicChunk(t *testing.T) {
	// 构造 chunked 帧: [size=5][hello][end=0x00000000]
	payload := []byte("hello")
	frame := makeChunkedFrame(payload)

	cr := NewChunkedReader(bytes.NewReader(frame), true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("expected %q, got %q", payload, got)
	}
}

// TestChunkedReader_EmptyChunk 测试空 chunk（只有结束标记）
func TestChunkedReader_EmptyChunk(t *testing.T) {
	// 构造: [end=0x00000000] 后紧接 EOF
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, chunkedEndMarker)

	cr := NewChunkedReader(&buf, true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty data, got %d bytes", len(got))
	}
}

// TestChunkedReader_DatastreamChunk 测试 Datastream 模式：单帧包含多个 packet 的数据
func TestChunkedReader_DatastreamChunk(t *testing.T) {
	// 模拟一个大帧包含两个 packet 的数据
	// Chunk = [size=13][packet1(5)+packet2(8)][end=0x00000000]
	payload := []byte("helloworld!!!")
	frame := makeChunkedFrame(payload)

	cr := NewChunkedReader(bytes.NewReader(frame), true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("expected %q, got %q", payload, got)
	}
}

// TestChunkedReader_MultipartChunk 测试 Multipart 模式：数据跨越多个 chunk part
func TestChunkedReader_MultipartChunk(t *testing.T) {
	// 构造 Multipart: [size=3][hel][size=2][lo][end=0x00000000]
	var buf bytes.Buffer
	// Part 1: size=3, data="hel"
	binary.Write(&buf, binary.LittleEndian, uint32(3))
	buf.Write([]byte("hel"))
	// Part 2: size=2, data="lo"
	binary.Write(&buf, binary.LittleEndian, uint32(2))
	buf.Write([]byte("lo"))
	// End marker
	binary.Write(&buf, binary.LittleEndian, chunkedEndMarker)

	cr := NewChunkedReader(bytes.NewReader(buf.Bytes()), true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("expected %q, got %q", "hello", string(got))
	}
}

// TestChunkedReader_MultipleConsecutiveChunks 测试连续多个独立 chunk
func TestChunkedReader_MultipleConsecutiveChunks(t *testing.T) {
	// [chunk1][chunk2][chunk3] 连续帧
	var buf bytes.Buffer
	payloads := []string{"first", "second", "third_payload_longer"}
	for _, p := range payloads {
		frame := makeChunkedFrame([]byte(p))
		buf.Write(frame)
	}

	cr := NewChunkedReader(bytes.NewReader(buf.Bytes()), true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	expected := strings.Join(payloads, "")
	if string(got) != expected {
		t.Errorf("expected %q, got %q", expected, string(got))
	}
}

// TestChunkedReader_LargePayload 测试大数据块（>64KB）
func TestChunkedReader_LargePayload(t *testing.T) {
	// 生成 128KB 随机数据
	payload := make([]byte, 128*1024)
	rand.Read(payload)

	frame := makeChunkedFrame(payload)
	cr := NewChunkedReader(bytes.NewReader(frame), true)

	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("payload mismatch: expected %d bytes, got %d bytes", len(payload), len(got))
	}
}

// TestChunkedReader_SmallReads 测试小缓冲区读取（每次只读 1-3 字节）
func TestChunkedReader_SmallReads(t *testing.T) {
	payload := []byte("hello world, chunked protocol test!")
	frame := makeChunkedFrame(payload)
	cr := NewChunkedReader(bytes.NewReader(frame), true)

	var result []byte
	buf := make([]byte, 3) // 每次最多读 3 字节
	for {
		n, err := cr.Read(buf)
		if n > 0 {
			result = append(result, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read error: %v", err)
		}
	}

	if !bytes.Equal(result, payload) {
		t.Errorf("expected %q, got %q", payload, result)
	}
}

// TestChunkedReader_Disabled 测试 disabled 模式（直接透传）
func TestChunkedReader_Disabled(t *testing.T) {
	rawData := []byte("raw data without chunked framing")
	cr := NewChunkedReader(bytes.NewReader(rawData), false)

	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if !bytes.Equal(got, rawData) {
		t.Errorf("disabled mode should passthrough: expected %q, got %q", rawData, got)
	}
}

// TestChunkedReader_InvalidFrameSize 测试帧大小超限
func TestChunkedReader_InvalidFrameSize(t *testing.T) {
	var buf bytes.Buffer
	// 帧大小 = maxChunkSize + 1
	binary.Write(&buf, binary.LittleEndian, uint32(maxChunkSize+1))

	cr := NewChunkedReader(bytes.NewReader(buf.Bytes()), true)
	_, err := io.ReadAll(cr)
	if err == nil {
		t.Error("expected error for oversized frame, got nil")
	}
	t.Logf("Got expected error: %v", err)
}

// TestChunkedReader_TruncatedHeader 测试帧头被截断（返回干净的 EOF）
func TestChunkedReader_TruncatedHeader(t *testing.T) {
	// 只有 2 字节，不够 4 字节帧头 → 应返回 io.EOF（干净结束）
	cr := NewChunkedReader(bytes.NewReader([]byte{0x05, 0x00}), true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Errorf("truncated header should result in clean EOF, got error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("truncated header should return no data, got %d bytes", len(got))
	}
	t.Log("Truncated header correctly treated as EOF")
}

// TestChunkedReader_TruncatedPayload 测试帧数据被截断
func TestChunkedReader_TruncatedPayload(t *testing.T) {
	var buf bytes.Buffer
	// 帧头说有 10 字节，但只提供 3 字节
	binary.Write(&buf, binary.LittleEndian, uint32(10))
	buf.Write([]byte("xyz"))

	cr := NewChunkedReader(bytes.NewReader(buf.Bytes()), true)
	_, err := io.ReadAll(cr)
	// 应该能读到 xyz 但在后续读取时 EOF
	if err != nil && err != io.EOF {
		t.Logf("Got error (expected EOF-related): %v", err)
	}
}

// TestChunkedReader_MultipartWithEndMarkersBetween 测试多个 chunk 间隔有结束标记
func TestChunkedReader_MultipartWithEndMarkersBetween(t *testing.T) {
	// [size=3][abc][end][size=3][def][end]
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(3))
	buf.Write([]byte("abc"))
	binary.Write(&buf, binary.LittleEndian, chunkedEndMarker)
	binary.Write(&buf, binary.LittleEndian, uint32(3))
	buf.Write([]byte("def"))
	binary.Write(&buf, binary.LittleEndian, chunkedEndMarker)

	cr := NewChunkedReader(bytes.NewReader(buf.Bytes()), true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if string(got) != "abcdef" {
		t.Errorf("expected %q, got %q", "abcdef", string(got))
	}
}

// ============================================================================
// ChunkedWriter 单元测试
// ============================================================================

// TestChunkedWriter_FrameFormat 验证输出帧的精确格式
func TestChunkedWriter_FrameFormat(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, true)

	payload := []byte("hello")
	n, err := cw.Write(payload)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != len(payload) {
		t.Errorf("expected write %d bytes, got %d", len(payload), n)
	}

	output := buf.Bytes()

	// 验证帧头: [5, 0, 0, 0]
	expectedSize := uint32(len(payload))
	gotSize := binary.LittleEndian.Uint32(output[:4])
	if gotSize != expectedSize {
		t.Errorf("frame header: expected size %d, got %d", expectedSize, gotSize)
	}

	// 验证数据
	gotPayload := output[4 : 4+len(payload)]
	if !bytes.Equal(gotPayload, payload) {
		t.Errorf("frame data: expected %q, got %q", payload, gotPayload)
	}

	// 验证结束标记: [0, 0, 0, 0]
	endMarker := binary.LittleEndian.Uint32(output[4+len(payload):])
	if endMarker != 0 {
		t.Errorf("end marker: expected 0, got %d", endMarker)
	}

	// 验证总长度
	expectedLen := 4 + len(payload) + 4
	if len(output) != expectedLen {
		t.Errorf("total length: expected %d, got %d", expectedLen, len(output))
	}
}

// TestChunkedWriter_MultipleWrites 测试连续多次写入
func TestChunkedWriter_MultipleWrites(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, true)

	writes := []string{"first", "second", "third"}
	for _, w := range writes {
		n, err := cw.Write([]byte(w))
		if err != nil {
			t.Fatalf("Write(%q) error: %v", w, err)
		}
		if n != len(w) {
			t.Errorf("Write(%q): expected %d bytes, got %d", w, len(w), n)
		}
	}

	// 验证输出包含 3 个完整帧
	data := buf.Bytes()
	offset := 0
	for i, w := range writes {
		if offset+4 > len(data) {
			t.Fatalf("frame %d: not enough data for header", i)
		}
		size := binary.LittleEndian.Uint32(data[offset:])
		if size != uint32(len(w)) {
			t.Errorf("frame %d: expected size %d, got %d", i, len(w), size)
		}
		offset += 4

		payload := string(data[offset : offset+int(size)])
		if payload != w {
			t.Errorf("frame %d: expected %q, got %q", i, w, payload)
		}
		offset += int(size)

		endMark := binary.LittleEndian.Uint32(data[offset:])
		if endMark != 0 {
			t.Errorf("frame %d: expected end marker 0, got %d", i, endMark)
		}
		offset += 4
	}
}

// TestChunkedWriter_Disabled 测试 disabled 模式
func TestChunkedWriter_Disabled(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, false)

	payload := []byte("raw passthrough data")
	n, err := cw.Write(payload)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != len(payload) {
		t.Errorf("expected write %d bytes, got %d", len(payload), n)
	}

	// disabled 模式不应该有帧头
	if !bytes.Equal(buf.Bytes(), payload) {
		t.Errorf("disabled mode should passthrough: expected %q, got %q", payload, buf.Bytes())
	}
}

// TestChunkedWriter_EmptyWrite 测试空数据写入
func TestChunkedWriter_EmptyWrite(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, true)

	n, err := cw.Write([]byte{})
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != 0 {
		t.Errorf("expected 0 bytes written, got %d", n)
	}
	if buf.Len() != 0 {
		t.Errorf("empty write should produce no output, got %d bytes", buf.Len())
	}
}

// TestChunkedWriter_LargePayload 测试大数据块写入
// R2-3: 超过 defaultChunkPayloadSize 的数据会被分为多个 chunk
func TestChunkedWriter_LargePayload(t *testing.T) {
	payload := make([]byte, 256*1024)
	rand.Read(payload)

	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, true)

	n, err := cw.Write(payload)
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != len(payload) {
		t.Errorf("expected %d bytes, got %d", len(payload), n)
	}

	// R2-3: 256KB / 64KB = 4 个 chunk，每个 chunk 有 header(4) + data + endMarker(4)
	expectedChunks := len(payload) / defaultChunkPayloadSize
	if len(payload)%defaultChunkPayloadSize != 0 {
		expectedChunks++
	}
	// 每个 chunk 的 overhead = 4 (header) + 4 (endMarker) = 8 bytes
	expectedLen := len(payload) + expectedChunks*8
	output := buf.Bytes()
	if len(output) != expectedLen {
		t.Errorf("expected total %d bytes, got %d (expected %d chunks)", expectedLen, len(output), expectedChunks)
	}

	// 验证第一个 chunk 的大小为 defaultChunkPayloadSize
	firstChunkSize := binary.LittleEndian.Uint32(output[:4])
	if int(firstChunkSize) != defaultChunkPayloadSize {
		t.Errorf("first chunk size: expected %d, got %d", defaultChunkPayloadSize, firstChunkSize)
	}

	// 验证 roundtrip
	cr := NewChunkedReader(bytes.NewReader(output), true)
	decoded, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}
	if !bytes.Equal(decoded, payload) {
		t.Errorf("roundtrip failed: expected %d bytes, got %d", len(payload), len(decoded))
	}
}

// ============================================================================
// Round-trip 测试（Write → Read 验证数据完整性）
// ============================================================================

// TestChunkedRoundTrip_Basic 基本 round-trip 测试
func TestChunkedRoundTrip_Basic(t *testing.T) {
	testCases := []struct {
		name    string
		payload string
	}{
		{"empty", ""},
		{"short", "hi"},
		{"medium", "hello world, this is a chunked protocol round-trip test!"},
		{"exact_boundary", strings.Repeat("x", 1024)},
		{"large", strings.Repeat("A", 100*1024)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.payload == "" {
				t.Skip("empty payload produces no frames")
			}

			var buf bytes.Buffer
			cw := NewChunkedWriter(&buf, true)

			_, err := cw.Write([]byte(tc.payload))
			if err != nil {
				t.Fatalf("Write error: %v", err)
			}

			cr := NewChunkedReader(bytes.NewReader(buf.Bytes()), true)
			got, err := io.ReadAll(cr)
			if err != nil {
				t.Fatalf("ReadAll error: %v", err)
			}

			if string(got) != tc.payload {
				t.Errorf("round-trip mismatch:\n  expected (%d bytes): %q\n  got     (%d bytes): %q",
					len(tc.payload), summarize(tc.payload, 50),
					len(got), summarize(string(got), 50))
			}
		})
	}
}

// TestChunkedRoundTrip_MultipleWrites 多次写入的 round-trip
func TestChunkedRoundTrip_MultipleWrites(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, true)

	writes := []string{
		"SELECT * FROM table",
		"WHERE id > 100",
		"\x01\x02\x03\x04\x05", // 二进制数据
		strings.Repeat("query_data_", 1000),
	}

	var expected bytes.Buffer
	for _, w := range writes {
		cw.Write([]byte(w))
		expected.WriteString(w)
	}

	cr := NewChunkedReader(bytes.NewReader(buf.Bytes()), true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if !bytes.Equal(got, expected.Bytes()) {
		t.Errorf("multi-write round-trip failed: expected %d bytes, got %d bytes",
			expected.Len(), len(got))
	}
}

// TestChunkedRoundTrip_BinaryData 二进制数据 round-trip（确保不受字节值影响）
func TestChunkedRoundTrip_BinaryData(t *testing.T) {
	// 生成包含所有 256 种字节值的数据
	payload := make([]byte, 256*4)
	for i := 0; i < 256; i++ {
		// 每个字节值重复 4 次，确保 0x00 不会被误认为帧结束标记
		payload[i*4] = byte(i)
		payload[i*4+1] = byte(i)
		payload[i*4+2] = byte(i)
		payload[i*4+3] = byte(i)
	}

	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, true)
	cw.Write(payload)

	cr := NewChunkedReader(bytes.NewReader(buf.Bytes()), true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if !bytes.Equal(got, payload) {
		t.Errorf("binary round-trip failed: expected %d bytes, got %d bytes", len(payload), len(got))
		// 找到第一个不匹配的位置
		for i := 0; i < len(payload) && i < len(got); i++ {
			if payload[i] != got[i] {
				t.Errorf("first mismatch at byte %d: expected 0x%02X, got 0x%02X", i, payload[i], got[i])
				break
			}
		}
	}
}

// TestChunkedRoundTrip_Disabled 非 chunked 模式的 round-trip
func TestChunkedRoundTrip_Disabled(t *testing.T) {
	payload := []byte("raw passthrough without chunked framing")

	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, false)
	cw.Write(payload)

	cr := NewChunkedReader(bytes.NewReader(buf.Bytes()), false)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if !bytes.Equal(got, payload) {
		t.Errorf("disabled round-trip failed: expected %q, got %q", payload, got)
	}
}

// ============================================================================
// 注意：ChunkedWriter 不再是并发安全的（P2 #8 移除了 Mutex）。
// 调用方必须保证单 goroutine 访问，这与实际使用场景一致。
// 原 TestChunkedWriter_Concurrent 测试已移除。
// ============================================================================

// ============================================================================
// chunkedNegotiate 测试
// ============================================================================

// TestChunkedNegotiate 测试协商逻辑
func TestChunkedNegotiate(t *testing.T) {
	testCases := []struct {
		sender   string
		receiver string
		expected bool
	}{
		{"chunked", "chunked", true},
		{"chunked", "notchunked", false},
		{"notchunked", "chunked", false},
		{"notchunked", "notchunked", false},
		{"", "", false},
		{"chunked", "", false},
		{"", "chunked", false},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("send=%q_recv=%q", tc.sender, tc.receiver), func(t *testing.T) {
			result := chunkedNegotiate(tc.sender, tc.receiver)
			if result != tc.expected {
				t.Errorf("chunkedNegotiate(%q, %q) = %v, expected %v",
					tc.sender, tc.receiver, result, tc.expected)
			}
		})
	}
}

// ============================================================================
// 模拟真实场景测试
// ============================================================================

// TestChunkedReader_SimulateClickHouseDataFlow 模拟 ClickHouse 数据流
// 模拟场景：ServerHello + Data blocks + EndOfStream 通过 chunked 传输
func TestChunkedReader_SimulateClickHouseDataFlow(t *testing.T) {
	var chunkedStream bytes.Buffer

	// 模拟 ClickHouse 服务器通过 chunked 发送三个"packet"
	packets := [][]byte{
		{0x00, 0x0E, 'C', 'l', 'i', 'c', 'k', 'H', 'o', 'u', 's', 'e', ' ', '2', '4', '.'}, // ServerHello 模拟
		{0x01, 0x05, 'h', 'e', 'l', 'l', 'o'},                                              // Data block 模拟
		{0x05},                                                                             // EndOfStream (type=5)
	}

	// 每个 packet 单独一个 chunk（Basic 模式）
	for _, pkt := range packets {
		frame := makeChunkedFrame(pkt)
		chunkedStream.Write(frame)
	}

	cr := NewChunkedReader(bytes.NewReader(chunkedStream.Bytes()), true)

	var result []byte
	buf := make([]byte, 4096)
	for {
		n, err := cr.Read(buf)
		if n > 0 {
			result = append(result, buf[:n]...)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read error: %v", err)
		}
	}

	// 验证所有 packet 数据都完整还原
	var expected []byte
	for _, pkt := range packets {
		expected = append(expected, pkt...)
	}

	if !bytes.Equal(result, expected) {
		t.Errorf("simulated data flow mismatch:\n  expected (%d bytes): %x\n  got     (%d bytes): %x",
			len(expected), expected[:minInt(32, len(expected))],
			len(result), result[:minInt(32, len(result))])
	}
}

// TestChunkedReader_SimulateDatastreamMode 模拟 Datastream 模式
// 一个 chunk 包含多个完整 packet
func TestChunkedReader_SimulateDatastreamMode(t *testing.T) {
	// 构造: [chunk_size=total][pkt1][pkt2][pkt3][end_marker]
	packet1 := []byte{0x01, 0x03, 'a', 'b', 'c'} // Data
	packet2 := []byte{0x01, 0x03, 'd', 'e', 'f'} // Data
	packet3 := []byte{0x05}                      // EndOfStream

	var combined []byte
	combined = append(combined, packet1...)
	combined = append(combined, packet2...)
	combined = append(combined, packet3...)

	frame := makeChunkedFrame(combined)

	cr := NewChunkedReader(bytes.NewReader(frame), true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if !bytes.Equal(got, combined) {
		t.Errorf("datastream mode mismatch: expected %x, got %x", combined, got)
	}
}

// TestChunkedReader_SimulateMultipartMode 模拟 Multipart 模式
// 一个大 packet 的数据被拆分到多个 chunk part 中
func TestChunkedReader_SimulateMultipartMode(t *testing.T) {
	// 模拟一个 1000 字节的 Data block 被拆分成 3 个 chunk part
	fullPayload := make([]byte, 1000)
	for i := range fullPayload {
		fullPayload[i] = byte(i % 256)
	}

	// Part 1: 前 400 字节
	// Part 2: 中间 300 字节
	// Part 3: 后 300 字节
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(400))
	buf.Write(fullPayload[:400])
	binary.Write(&buf, binary.LittleEndian, uint32(300))
	buf.Write(fullPayload[400:700])
	binary.Write(&buf, binary.LittleEndian, uint32(300))
	buf.Write(fullPayload[700:])
	binary.Write(&buf, binary.LittleEndian, chunkedEndMarker)

	cr := NewChunkedReader(bytes.NewReader(buf.Bytes()), true)
	got, err := io.ReadAll(cr)
	if err != nil {
		t.Fatalf("ReadAll error: %v", err)
	}

	if !bytes.Equal(got, fullPayload) {
		t.Errorf("multipart mode failed: expected %d bytes, got %d bytes", len(fullPayload), len(got))
	}
}

// TestChunked_EnabledVsDisabled_Comparison 对比 chunked 和非 chunked 模式的行为差异
func TestChunked_EnabledVsDisabled_Comparison(t *testing.T) {
	payload := []byte("SELECT * FROM system.tables WHERE database = 'default'")

	// Chunked 模式
	var chunkedBuf bytes.Buffer
	cwEnabled := NewChunkedWriter(&chunkedBuf, true)
	cwEnabled.Write(payload)

	// 非 Chunked 模式
	var rawBuf bytes.Buffer
	cwDisabled := NewChunkedWriter(&rawBuf, false)
	cwDisabled.Write(payload)

	// 验证 chunked 模式产生的数据比原始数据大（有帧头 + 结束标记 = +8 bytes）
	chunkedOverhead := chunkedBuf.Len() - rawBuf.Len()
	expectedOverhead := 8 // 4 bytes header + 4 bytes end marker
	if chunkedOverhead != expectedOverhead {
		t.Errorf("chunked overhead: expected %d bytes, got %d bytes", expectedOverhead, chunkedOverhead)
	}

	// 验证 chunked 数据可以正确解帧还原
	crEnabled := NewChunkedReader(bytes.NewReader(chunkedBuf.Bytes()), true)
	gotChunked, _ := io.ReadAll(crEnabled)

	crDisabled := NewChunkedReader(bytes.NewReader(rawBuf.Bytes()), false)
	gotRaw, _ := io.ReadAll(crDisabled)

	if !bytes.Equal(gotChunked, gotRaw) {
		t.Error("chunked and raw should produce identical decoded data")
	}

	if !bytes.Equal(gotChunked, payload) {
		t.Error("decoded data should match original payload")
	}

	t.Logf("Payload: %d bytes, Chunked wire: %d bytes, Raw wire: %d bytes, Overhead: %d bytes",
		len(payload), chunkedBuf.Len(), rawBuf.Len(), chunkedOverhead)
}

// ============================================================================
// 辅助函数
// ============================================================================

// makeChunkedFrame 构造一个完整的 chunked 帧：[size][payload][end_marker]
func makeChunkedFrame(payload []byte) []byte {
	var buf bytes.Buffer
	binary.Write(&buf, binary.LittleEndian, uint32(len(payload)))
	buf.Write(payload)
	binary.Write(&buf, binary.LittleEndian, chunkedEndMarker)
	return buf.Bytes()
}

func summarize(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// minInt 返回两个 int 的较小值（避免与其他测试文件重名）
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
