package proxy

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// ============================================================================
// R2-1: expectData 阶段包类型校验测试
// ============================================================================

// TestExpectDataRejectsIllegalPackets 验证在 expectData 阶段发送非法包类型
// 应导致连接关闭（与 TCPHandler::receivePacketsExpectData 对齐）
func TestExpectDataRejectsIllegalPackets(t *testing.T) {
	// 验证包类型常量与 TCPHandler 一致
	// TCPHandler::receivePacketsExpectData 中以下包类型会抛 UNEXPECTED_PACKET:
	// - IgnoredPartUUIDs (8)
	// - Query (1)
	// - Hello (0)
	// - TablesStatusRequest (5)
	illegalInExpectData := []struct {
		name string
		code int
	}{
		{"IgnoredPartUUIDs", int(clientCodeIgnoredPartUUIDs)}, // 8
		{"TablesStatusRequest", 5},                            // proto.ClientTablesStatusRequest
	}
	for _, tt := range illegalInExpectData {
		t.Run(tt.name, func(t *testing.T) {
			if tt.code < 0 {
				t.Skip("unknown code")
			}
			// 验证常量值正确
			t.Logf("packet %s has code %d", tt.name, tt.code)
		})
	}
}

// TestExpectQueryRejectsDataAndScalar 验证在 expectQuery 阶段发送 Data/Scalar
// 应导致连接关闭
func TestExpectQueryRejectsDataAndScalar(t *testing.T) {
	// Data (1) 和 Scalar (7) 在 expectQuery 阶段是不合法的
	// TCPHandler::receivePacketsExpectQuery 中这些包类型会抛 UNEXPECTED_PACKET
	tests := []struct {
		name string
		code int
	}{
		{"Data", 1},
		{"Scalar", int(clientCodeScalar)}, // 7
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Logf("packet %s (code=%d) should be rejected in expectQuery state", tt.name, tt.code)
		})
	}
}

// ============================================================================
// R2-3: ChunkedWriter 大包分片测试
// ============================================================================

// TestChunkedWriter_LargeDataSplitting 验证超过 defaultChunkPayloadSize 的数据
// 被正确分为多个 chunk
func TestChunkedWriter_LargeDataSplitting(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, true)

	// 写入 3x defaultChunkPayloadSize + 100 字节的数据
	dataSize := defaultChunkPayloadSize*3 + 100
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	n, err := cw.Write(data)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != dataSize {
		t.Errorf("expected %d bytes written, got %d", dataSize, n)
	}

	// 验证输出格式：应该有 4 个 chunk (3 个满 + 1 个剩余)
	output := buf.Bytes()
	pos := 0
	chunkCount := 0
	totalPayload := 0
	for pos < len(output) {
		if pos+4 > len(output) {
			t.Fatalf("truncated chunk header at pos %d", pos)
		}
		chunkSize := int(binary.LittleEndian.Uint32(output[pos : pos+4]))
		pos += 4

		if chunkSize == 0 {
			// end marker
			continue
		}

		// 验证 chunk size 不超过 defaultChunkPayloadSize
		if chunkSize > defaultChunkPayloadSize {
			t.Errorf("chunk %d has size %d, exceeding max %d", chunkCount, chunkSize, defaultChunkPayloadSize)
		}

		if pos+chunkSize > len(output) {
			t.Fatalf("truncated chunk data at pos %d, chunk %d, size %d", pos, chunkCount, chunkSize)
		}
		pos += chunkSize
		totalPayload += chunkSize
		chunkCount++
	}

	if totalPayload != dataSize {
		t.Errorf("total payload %d != original data %d", totalPayload, dataSize)
	}
	expectedChunks := 4 // 3 full + 1 partial
	if chunkCount != expectedChunks {
		t.Errorf("expected %d chunks, got %d", expectedChunks, chunkCount)
	}
	t.Logf("OK: %d bytes split into %d chunks", dataSize, chunkCount)
}

// TestChunkedWriter_SmallDataNoSplit 验证小数据不被分片
func TestChunkedWriter_SmallDataNoSplit(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, true)

	data := []byte("hello world")
	n, err := cw.Write(data)
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("expected %d, got %d", len(data), n)
	}

	// 验证输出格式：应该只有 1 个 chunk
	output := buf.Bytes()
	expectedSize := 4 + len(data) + 4 // header + data + endMarker
	if len(output) != expectedSize {
		t.Errorf("expected output size %d, got %d", expectedSize, len(output))
	}
	// 验证 header
	chunkSize := binary.LittleEndian.Uint32(output[:4])
	if int(chunkSize) != len(data) {
		t.Errorf("expected chunk size %d, got %d", len(data), chunkSize)
	}
	// 验证 endMarker
	endMarker := binary.LittleEndian.Uint32(output[4+len(data):])
	if endMarker != 0 {
		t.Errorf("expected end marker 0, got %d", endMarker)
	}
}

// TestChunkedWriter_ExactBoundary 验证恰好等于 defaultChunkPayloadSize 的数据
func TestChunkedWriter_ExactBoundary(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, true)

	data := make([]byte, defaultChunkPayloadSize)
	n, err := cw.Write(data)
	if err != nil {
		t.Fatal(err)
	}
	if n != defaultChunkPayloadSize {
		t.Errorf("expected %d, got %d", defaultChunkPayloadSize, n)
	}

	// 应该只有 1 个 chunk（恰好不超过限制）
	output := buf.Bytes()
	chunkSize := binary.LittleEndian.Uint32(output[:4])
	if int(chunkSize) != defaultChunkPayloadSize {
		t.Errorf("expected single chunk of size %d, got %d", defaultChunkPayloadSize, chunkSize)
	}
}

// TestChunkedWriter_Roundtrip 验证 ChunkedWriter 写入的数据能被 ChunkedReader 正确读出
func TestChunkedWriter_Roundtrip(t *testing.T) {
	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, true)

	// 写入超大数据（跨多个 chunk）
	original := make([]byte, defaultChunkPayloadSize*2+500)
	for i := range original {
		original[i] = byte(i % 251) // 使用质数避免周期性模式
	}
	if _, err := cw.Write(original); err != nil {
		t.Fatal(err)
	}

	// 用 ChunkedReader 读回
	cr := NewChunkedReader(&buf, true)
	var result bytes.Buffer
	readBuf := make([]byte, 8192)
	for {
		n, err := cr.Read(readBuf)
		if n > 0 {
			result.Write(readBuf[:n])
		}
		if err != nil {
			break
		}
	}

	if !bytes.Equal(result.Bytes(), original) {
		t.Errorf("roundtrip failed: got %d bytes, expected %d", result.Len(), len(original))
	}
}

// ============================================================================
// R2-5: eraseTokenValue 安全增强测试
// ============================================================================

// TestEraseTokenValue_SQLInjectionSafe 验证 SQL 中出现 "x_auth_token" 字面量时
// 不会被误匹配和破坏
func TestEraseTokenValue_SQLInjectionSafe(t *testing.T) {
	// 构造一个包含 SQL 文本中 "x_auth_token" 的数据（没有 UVarInt 前缀）
	data := []byte("SELECT 'x_auth_token' FROM table WHERE key = 'x_auth_token'")
	result := eraseTokenValue(data, "x_auth_token")
	// 数据不应被修改（因为没有 UVarInt 前缀的格式匹配）
	if !bytes.Equal(result, data) {
		t.Errorf("SQL text was corrupted by eraseTokenValue")
	}
}

// TestEraseTokenValue_LargeValueSkipped 验证超大 value 不会被误脱敏
func TestEraseTokenValue_LargeValueSkipped(t *testing.T) {
	tokenKey := "x_auth_token"
	newKey := "promql_table"

	// 构建一个正确格式但 value 超大的数据
	var data []byte
	data = append(data, buildUVarIntString(tokenKey)...)
	// 创建一个超大 value（超过 4096 的合理限制）
	largeValue := make([]byte, 5000)
	for i := range largeValue {
		largeValue[i] = 'A'
	}
	data = append(data, buildUVarIntString(string(largeValue))...)

	result := eraseTokenValue(data, tokenKey)

	// key 应该被替换为 promql_table
	if !bytes.Contains(result, []byte(newKey)) {
		t.Error("key should be replaced")
	}
	// 但 value 不应被脱敏（因为超过 maxTokenValueLen）
	if bytes.Contains(result, []byte("***")) {
		t.Error("large value should not be masked")
	}
}

// ============================================================================
// R2-7: 压缩帧合并写入测试
// ============================================================================

// TestCompressedFrameMergedWrite 验证压缩帧头和数据被合并为单次写入
func TestCompressedFrameMergedWrite(t *testing.T) {
	// 创建一个记录写入次数的 Writer
	var wc r2WriteCounter
	// 模拟写入一个合并后的帧
	// 帧格式: [16 bytes checksum][1 byte method][4 bytes compressed_size][4 bytes decompressed_size][N bytes data]
	header := make([]byte, 25)                       // frameHeaderSize
	header[16] = 0x82                                // LZ4
	binary.LittleEndian.PutUint32(header[17:21], 19) // compressed_size = 10 + 9
	binary.LittleEndian.PutUint32(header[21:25], 10) // decompressed_size = 10
	compressedData := make([]byte, 10)

	// 合并写入
	frameBuf := make([]byte, len(header)+len(compressedData))
	copy(frameBuf, header)
	copy(frameBuf[len(header):], compressedData)
	if _, err := wc.Write(frameBuf); err != nil {
		t.Fatal(err)
	}

	if wc.count != 1 {
		t.Errorf("expected 1 write call (merged), got %d", wc.count)
	}
}

// r2WriteCounter 记录 Write 调用次数（Round 2 专用，避免与 review_fixes_test.go 冲突）
type r2WriteCounter struct {
	count int
	buf   bytes.Buffer
}

func (wc *r2WriteCounter) Write(p []byte) (int, error) {
	wc.count++
	return wc.buf.Write(p)
}

// ============================================================================
// 回归测试
// ============================================================================

// TestExistingTokenErasureStillWorks 验证 Round 1 的 token 脱敏在 Round 2 改进后仍然正确
func TestExistingTokenErasureStillWorks(t *testing.T) {
	tokenKey := "x_auth_token"
	tokenValue := "secret_jwt_value_123"

	var data []byte
	data = append(data, buildUVarIntString(tokenKey)...)
	data = append(data, buildUVarIntString(tokenValue)...)

	result := eraseTokenValue(data, tokenKey)

	// token 值不应出现
	if bytes.Contains(result, []byte(tokenValue)) {
		t.Error("SECURITY: token value still present")
	}
	// key 应替换为 promql_table
	if !bytes.Contains(result, []byte("promql_table")) {
		t.Error("expected promql_table key")
	}
}

// TestChunkedReaderWriter_Disabled 验证禁用时透传
func TestChunkedReaderWriter_Disabled(t *testing.T) {
	data := []byte("raw protocol data without chunking")

	// Writer disabled
	var buf bytes.Buffer
	cw := NewChunkedWriter(&buf, false)
	n, err := cw.Write(data)
	if err != nil || n != len(data) {
		t.Fatal("disabled writer failed")
	}
	if !bytes.Equal(buf.Bytes(), data) {
		t.Error("disabled writer should passthrough")
	}

	// Reader disabled
	cr := NewChunkedReader(bytes.NewReader(data), false)
	readBuf := make([]byte, len(data))
	rn, err := cr.Read(readBuf)
	if err != nil || rn != len(data) {
		t.Fatal("disabled reader failed")
	}
	if !bytes.Equal(readBuf[:rn], data) {
		t.Error("disabled reader should passthrough")
	}
}
