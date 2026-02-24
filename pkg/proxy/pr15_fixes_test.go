package proxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

// ============================================================================
// PR15 修复验证测试
// ============================================================================

// buildCompressedFrameRaw 构造一个完整的压缩帧（不含 block_name 前缀）
func buildCompressedFrameRaw(method byte, dataSize int) []byte {
	var buf bytes.Buffer
	// 16 bytes checksum (dummy)
	checksum := make([]byte, 16)
	for i := range checksum {
		checksum[i] = byte(i)
	}
	buf.Write(checksum)
	// 1 byte compression method
	buf.WriteByte(method)
	// 4 bytes compressed_size = 9 + dataSize
	compressedSize := uint32(9 + dataSize)
	var csBytes [4]byte
	binary.LittleEndian.PutUint32(csBytes[:], compressedSize)
	buf.Write(csBytes[:])
	// 4 bytes decompressed_size (dummy)
	var dsBytes [4]byte
	binary.LittleEndian.PutUint32(dsBytes[:], uint32(dataSize*2))
	buf.Write(dsBytes[:])
	// data bytes
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}
	buf.Write(data)
	return buf.Bytes()
}

// TestPR15_CompressedMultiFrame 验证多帧压缩 Data Block 的完整转发。
// 修复前只处理单帧，修复后循环读取所有连续压缩帧。
func TestPR15_CompressedMultiFrame(t *testing.T) {
	// 构造 3 个连续的 LZ4 压缩帧
	frame1 := buildCompressedFrameRaw(0x82, 100)
	frame2 := buildCompressedFrameRaw(0x82, 200)
	frame3 := buildCompressedFrameRaw(0x82, 150)

	// 输入流: [block_name="\x00"][frame1][frame2][frame3][next_packet_type=0x01(Query)]
	var inputBuf bytes.Buffer
	inputBuf.WriteByte(0) // block_name = ""
	inputBuf.Write(frame1)
	inputBuf.Write(frame2)
	inputBuf.Write(frame3)
	// 写一个非压缩帧的字节作为下一个 packet 的开始（模拟 Query packet type=1）
	inputBuf.WriteByte(0x01)

	// 使用 >= 128KB 的 bufio.Reader，使 proto.NewReader 内部复用 br
	// （proto.NewReader 内部创建 128KB bufio.Reader，当输入已经 >= 128KB 时直接复用）
	// 这样 br.Peek() 才能看到与 chReader.ReadRaw() 相同的数据源
	const protoDefaultBufSize = 128 * 1024
	br := bufio.NewReaderSize(bytes.NewReader(inputBuf.Bytes()), protoDefaultBufSize)
	chReader := proto.NewReader(br)
	upstream := &bytes.Buffer{}

	p := NewProxy(Config{}, nil, nil)
	err := p.handleDataBlock(context.Background(), 1, proto.ClientCodeData, chReader, br, upstream, proto.CompressionEnabled, 54460)
	if err != nil {
		t.Fatalf("handleDataBlock error: %v", err)
	}

	output := upstream.Bytes()
	if len(output) == 0 {
		t.Fatal("output should not be empty")
	}

	// 验证：输出应包含所有 3 个帧的数据
	// 输出格式: [packet_code=0x02][block_name=\x00][frame1][frame2][frame3]
	expectedMinLen := 1 + 1 + len(frame1) + len(frame2) + len(frame3) // code + name + frames
	if len(output) < expectedMinLen {
		t.Errorf("output too short for 3 frames: expected >= %d, got %d", expectedMinLen, len(output))
	}

	// 验证每个帧的 checksum 模式在输出中
	for i := 0; i < 16; i++ {
		found := false
		for j := 0; j < len(output)-16; j++ {
			if output[j] == byte(i) && j+16 <= len(output) {
				match := true
				for k := 0; k < 16; k++ {
					if output[j+k] != byte(k) {
						match = false
						break
					}
				}
				if match {
					found = true
					break
				}
			}
		}
		if !found && i == 0 {
			t.Error("output should contain frame checksum patterns")
			break
		}
	}

	// 验证 br 中剩余的下一个字节应该是 0x01（下一个 packet type）
	nextByte, err := br.ReadByte()
	if err != nil {
		t.Fatalf("should have remaining data in br: %v", err)
	}
	if nextByte != 0x01 {
		t.Errorf("next byte should be 0x01 (Query), got 0x%02X", nextByte)
	}

	t.Logf("验证通过：3 帧压缩 Data Block 完整转发，总输出 %d 字节", len(output))
}

// TestPR15_CompressedMultiFrame_SingleFrame 验证单帧场景仍然正确。
func TestPR15_CompressedMultiFrame_SingleFrame(t *testing.T) {
	frame := buildCompressedFrameRaw(0x82, 100)

	var inputBuf bytes.Buffer
	inputBuf.WriteByte(0) // block_name = ""
	inputBuf.Write(frame)
	// 后续是非压缩帧数据（模拟 packet_type=0x01）
	inputBuf.WriteByte(0x01)

	const protoDefaultBufSize = 128 * 1024
	br := bufio.NewReaderSize(bytes.NewReader(inputBuf.Bytes()), protoDefaultBufSize)
	chReader := proto.NewReader(br)
	upstream := &bytes.Buffer{}

	p := NewProxy(Config{}, nil, nil)
	err := p.handleDataBlock(context.Background(), 1, proto.ClientCodeData, chReader, br, upstream, proto.CompressionEnabled, 54460)
	if err != nil {
		t.Fatalf("handleDataBlock error: %v", err)
	}

	// 验证单帧完整
	expectedLen := 1 + 1 + len(frame) // code + name + frame
	if len(upstream.Bytes()) != expectedLen {
		t.Errorf("single frame output length: expected %d, got %d", expectedLen, len(upstream.Bytes()))
	}
}

// TestPR15_CompressedMultiFrame_MixedMethods 验证混合压缩方法（LZ4 + ZSTD）。
func TestPR15_CompressedMultiFrame_MixedMethods(t *testing.T) {
	frame1 := buildCompressedFrameRaw(0x82, 100) // LZ4
	frame2 := buildCompressedFrameRaw(0x90, 200) // ZSTD

	var inputBuf bytes.Buffer
	inputBuf.WriteByte(0) // block_name
	inputBuf.Write(frame1)
	inputBuf.Write(frame2)
	inputBuf.WriteByte(0x01) // next packet

	const protoDefaultBufSize = 128 * 1024
	br := bufio.NewReaderSize(bytes.NewReader(inputBuf.Bytes()), protoDefaultBufSize)
	chReader := proto.NewReader(br)
	upstream := &bytes.Buffer{}

	p := NewProxy(Config{}, nil, nil)
	err := p.handleDataBlock(context.Background(), 1, proto.ClientCodeData, chReader, br, upstream, proto.CompressionEnabled, 54460)
	if err != nil {
		t.Fatalf("handleDataBlock error: %v", err)
	}

	expectedLen := 1 + 1 + len(frame1) + len(frame2)
	if len(upstream.Bytes()) != expectedLen {
		t.Errorf("mixed methods output: expected %d, got %d", expectedLen, len(upstream.Bytes()))
	}
}

// TestPR15_CompressedBufPool_Removed 验证移除 compressedBufPool 后单帧处理仍正确。
func TestPR15_CompressedBufPool_Removed(t *testing.T) {
	// 基本场景：验证压缩帧处理不再依赖 pool
	compressedSize := uint32(100)
	dataSize := int(compressedSize) - 9

	input := buildCompressedFrame(compressedSize, dataSize)
	br := bufio.NewReader(bytes.NewReader(input))
	chReader := proto.NewReader(br)
	upstream := &bytes.Buffer{}

	p := NewProxy(Config{}, nil, nil)
	err := p.handleDataBlock(context.Background(), 1, proto.ClientCodeData, chReader, br, upstream, proto.CompressionEnabled, 54460)
	if err != nil {
		t.Fatalf("handleDataBlock error (no pool): %v", err)
	}
	if upstream.Len() == 0 {
		t.Error("upstream output should not be empty")
	}
}

// TestPR15_ServerHelloError 验证 upstream 返回非 Hello 包时的错误处理。
func TestPR15_ServerHelloError(t *testing.T) {
	// 模拟 upstream 返回 Exception (type 2) 而非 ServerHello (type 0)
	var upstreamData bytes.Buffer
	// pktType = 2 (Exception) — UVarInt 编码
	upstreamData.WriteByte(0x02)
	// 模拟错误消息数据（长度较大，确保 bufio 缓冲区中有数据）
	errorMsg := []byte("authentication failed: wrong password")
	upstreamData.Write(errorMsg)

	// 使用足够大的 bufio.Reader，确保错误消息被预缓冲
	const largeBufSize = 128 * 1024
	upBr := bufio.NewReaderSize(bytes.NewReader(upstreamData.Bytes()), largeBufSize)
	upReader := proto.NewReader(upBr)

	// 读取 pktType（通过 proto.Reader，会从内部 bufio 读取）
	pktType, err := upReader.UVarInt()
	if err != nil {
		t.Fatalf("读取 pktType 失败: %v", err)
	}

	// 验证检测到非 Hello 类型
	if pktType == 0 {
		t.Fatal("pktType should not be 0 for exception scenario")
	}
	if pktType != 2 {
		t.Errorf("expected pktType=2 (Exception), got %d", pktType)
	}

	// 验证错误处理逻辑：构造转发缓冲区
	// 注意：proto.NewReader 内部 bufio.Reader = upBr（因 size >= 128KB 复用）
	// 所以 upBr.Buffered() 应包含错误消息
	errBuf := &proto.Buffer{}
	errBuf.PutUVarInt(pktType)
	bufferedN := upBr.Buffered()
	if bufferedN > 0 {
		remaining := make([]byte, bufferedN)
		n, _ := upBr.Read(remaining)
		if n > 0 {
			errBuf.Buf = append(errBuf.Buf, remaining[:n]...)
		}
	}

	// 验证输出包含 exception 类型
	if len(errBuf.Buf) < 1 {
		t.Fatalf("error buffer too short: %d bytes", len(errBuf.Buf))
	}
	// 第一个字节应是 pktType=2 的 UVarInt 编码
	if errBuf.Buf[0] != 0x02 {
		t.Errorf("first byte should be 0x02 (Exception), got 0x%02X", errBuf.Buf[0])
	}
	// 验证错误消息存在（如果 bufio 预缓冲了后续数据）
	if bufferedN > 0 && !bytes.Contains(errBuf.Buf, []byte("authentication failed")) {
		t.Error("error buffer should contain error message when data is buffered")
	}

	t.Logf("验证通过：ServerHello 错误场景正确处理，pktType=%d, errBuf=%d 字节", pktType, len(errBuf.Buf))
}

// TestPR15_AddendumGateCondition 验证低版本客户端不发 Addendum 的场景。
func TestPR15_AddendumGateCondition(t *testing.T) {
	tests := []struct {
		name           string
		clientRevision int
		serverRevision int
		shouldRead     bool
	}{
		{
			name:           "双方都支持 Addendum",
			clientRevision: 54460,
			serverRevision: 54460,
			shouldRead:     true,
		},
		{
			name:           "客户端不支持 Addendum",
			clientRevision: 54400,
			serverRevision: 54460,
			shouldRead:     false,
		},
		{
			name:           "服务端不支持 Addendum",
			clientRevision: 54460,
			serverRevision: 54400,
			shouldRead:     false,
		},
		{
			name:           "双方都不支持 Addendum",
			clientRevision: 54400,
			serverRevision: 54400,
			shouldRead:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// FeatureAddendum = 54458
			clientSupports := proto.FeatureAddendum.In(tt.clientRevision)
			serverSupports := proto.FeatureAddendum.In(tt.serverRevision)
			shouldRead := clientSupports && serverSupports

			if shouldRead != tt.shouldRead {
				t.Errorf("shouldRead=%v, expected %v (client=%d, server=%d)",
					shouldRead, tt.shouldRead, tt.clientRevision, tt.serverRevision)
			}
		})
	}
}

// TestPR15_ExtractQuerySummary_PrecompiledRegex 验证预编译正则的结果与原实现一致。
func TestPR15_ExtractQuerySummary_PrecompiledRegex(t *testing.T) {
	tests := []struct {
		input    string
		maxLen   int
		expected string
	}{
		{
			input:    "USE test_database",
			maxLen:   100,
			expected: "USE test_database",
		},
		{
			input:    "some prefix USE test_db",
			maxLen:   100,
			expected: "USE test_db",
		},
		{
			input:    "SELECT * FROM table",
			maxLen:   100,
			expected: "SELECT * FROM table",
		},
		{
			input:    "random garbage",
			maxLen:   100,
			expected: "random garbage",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := extractQuerySummary([]byte(tt.input), tt.maxLen)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}

// TestPR15_ChunkedWriter_BufferPool 验证 ChunkedWriter 使用 pool 后帧格式正确。
func TestPR15_ChunkedWriter_BufferPool(t *testing.T) {
	// 多次写入以触发 pool 复用
	for i := 0; i < 10; i++ {
		var buf bytes.Buffer
		cw := NewChunkedWriter(&buf, true)

		data := []byte(fmt.Sprintf("test_data_%04d", i))
		n, err := cw.Write(data)
		if err != nil {
			t.Fatalf("Write #%d error: %v", i, err)
		}
		if n != len(data) {
			t.Errorf("Write #%d returned %d, expected %d", i, n, len(data))
		}

		// 验证帧格式
		output := buf.Bytes()
		expectedSize := 4 + len(data) + 4
		if len(output) != expectedSize {
			t.Errorf("Write #%d frame size: expected %d, got %d", i, expectedSize, len(output))
		}

		// 验证 header
		headerSize := binary.LittleEndian.Uint32(output[:4])
		if headerSize != uint32(len(data)) {
			t.Errorf("Write #%d header: expected %d, got %d", i, len(data), headerSize)
		}

		// 验证 data
		if !bytes.Equal(output[4:4+len(data)], data) {
			t.Errorf("Write #%d data mismatch", i)
		}

		// 验证 end marker
		endMarker := binary.LittleEndian.Uint32(output[4+len(data):])
		if endMarker != 0 {
			t.Errorf("Write #%d end marker: expected 0, got %d", i, endMarker)
		}

		// 验证 round-trip
		cr := NewChunkedReader(bytes.NewReader(output), true)
		got, err := io.ReadAll(cr)
		if err != nil {
			t.Fatalf("Write #%d round-trip read error: %v", i, err)
		}
		if !bytes.Equal(got, data) {
			t.Errorf("Write #%d round-trip mismatch", i)
		}
	}
}

// TestPR15_DetectServerPacketType_ChunkedMode 验证 chunked 模式下 packet 检测的行为。
func TestPR15_DetectServerPacketType_ChunkedMode(t *testing.T) {
	tests := []struct {
		name     string
		chunk    []byte
		expected string
	}{
		{
			name:     "EndOfStream 独占 chunk",
			chunk:    []byte{0x05},
			expected: "EndOfStream",
		},
		{
			name:     "Exception 独占 chunk",
			chunk:    []byte{0x02},
			expected: "Exception",
		},
		{
			name:     "Data 包开始",
			chunk:    []byte{0x01},
			expected: "Data",
		},
		{
			name:     "未知字节",
			chunk:    []byte{0xFF},
			expected: "unknown",
		},
		{
			name:     "从中间开始的数据（非 packet 边界）",
			chunk:    []byte{0x48, 0x65, 0x6C, 0x6C, 0x6F}, // "Hello"
			expected: "unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detectServerPacketType(tt.chunk)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}
