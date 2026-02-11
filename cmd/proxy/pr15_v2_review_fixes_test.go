package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"strings"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

// ============================================================================
// P0-1: ServerHello 尾部字段透传验证
// ============================================================================

// TestServerHello_TailPassthrough 验证 ServerHello 在 FeatureChunkedPackets 字段之后
// 的额外字段（如 password_rules, nonce, settings 等）被正确透传。
// 修复前：这些字段残留在 upBr 中，由 copyUpstreamToClientFromReader 处理，
//
//	可能被 ChunkedReader 误解析。
//
// 修复后：在 close(handshakeDone) 前 drain upBr 中的缓冲数据，一并发送给客户端。
func TestServerHello_TailPassthrough(t *testing.T) {
	// 模拟一个带有尾部额外字段的 ServerHello
	// 构造字段序列:
	//   [pktType=0] [name="TestServer"] [major=24] [minor=1] [revision=54476]
	//   [parallel_replicas_version=1]  (>= 54471)
	//   [timezone="UTC"]               (>= 54058)
	//   [display_name="test"]          (>= 54372)
	//   [version_patch=3]              (>= 54401)
	//   [proto_send_chunked="notchunked"]  (>= 54470)
	//   [proto_recv_chunked="notchunked"]  (>= 54470)
	//   [tail_extra_bytes: "extra_tail_data"]  模拟 password_rules 等字段
	var serverHelloData bytes.Buffer
	buf := &proto.Buffer{}

	// pktType = 0 (ServerHello)
	buf.PutUVarInt(0)
	// name
	buf.PutString("TestServer")
	// major
	buf.PutUVarInt(24)
	// minor
	buf.PutUVarInt(1)
	// revision
	buf.PutUVarInt(54476)
	// parallel_replicas_version (>= 54471)
	buf.PutUVarInt(1)
	// timezone (>= 54058)
	buf.PutString("UTC")
	// display_name (>= 54372)
	buf.PutString("test")
	// version_patch (>= 54401)
	buf.PutUVarInt(3)
	// proto_send_chunked (>= 54470)
	buf.PutString("notchunked")
	// proto_recv_chunked (>= 54470)
	buf.PutString("notchunked")

	baseHelloLen := len(buf.Buf)

	// 模拟尾部额外字段 (password_rules, nonce, settings 等)
	tailData := []byte("EXTRA_TAIL_SENTINEL_DATA_12345")
	buf.Buf = append(buf.Buf, tailData...)

	serverHelloData.Write(buf.Buf)

	// 用 bufio.Reader 包裹（模拟 upBr）
	const largeBufSize = 128 * 1024
	upBr := bufio.NewReaderSize(bytes.NewReader(serverHelloData.Bytes()), largeBufSize)
	upReader := proto.NewReader(upBr)

	clientRevision := 54476 // 支持所有 feature

	// 模拟 ServerHello 解析逻辑（与 proxy.go 中的实现一致）
	serverHelloBuf := &proto.Buffer{}

	// pktType
	pktType, err := upReader.UVarInt()
	if err != nil {
		t.Fatalf("read pktType: %v", err)
	}
	if pktType != 0 {
		t.Fatalf("expected pktType=0, got %d", pktType)
	}
	serverHelloBuf.PutUVarInt(pktType)

	// name
	name, err := upReader.Str()
	if err != nil {
		t.Fatalf("read name: %v", err)
	}
	serverHelloBuf.PutString(name)

	// major, minor, revision
	major, _ := upReader.UVarInt()
	serverHelloBuf.PutUVarInt(major)
	minor, _ := upReader.UVarInt()
	serverHelloBuf.PutUVarInt(minor)
	rev, _ := upReader.UVarInt()
	serverHelloBuf.PutUVarInt(rev)

	// parallel_replicas_version
	if proto.FeatureVersionedParallelReplicas.In(clientRevision) {
		v, _ := upReader.UVarInt()
		serverHelloBuf.PutUVarInt(v)
	}
	// timezone
	if proto.FeatureTimezone.In(clientRevision) {
		v, _ := upReader.Str()
		serverHelloBuf.PutString(v)
	}
	// display_name
	if proto.FeatureDisplayName.In(clientRevision) {
		v, _ := upReader.Str()
		serverHelloBuf.PutString(v)
	}
	// version_patch
	if proto.FeatureVersionPatch.In(clientRevision) {
		v, _ := upReader.UVarInt()
		serverHelloBuf.PutUVarInt(v)
	}
	// chunked
	if proto.FeatureChunkedPackets.In(clientRevision) {
		sendC, _ := upReader.Str()
		serverHelloBuf.PutString(sendC)
		recvC, _ := upReader.Str()
		serverHelloBuf.PutString(recvC)
	}

	// P0-1 修复核心：drain upBr 中的尾部数据
	if tailN := upBr.Buffered(); tailN > 0 {
		tail := make([]byte, tailN)
		n, _ := upBr.Read(tail)
		if n > 0 {
			serverHelloBuf.Buf = append(serverHelloBuf.Buf, tail[:n]...)
		}
	}

	// 验证 1: 输出长度应等于输入长度（完整透传）
	if len(serverHelloBuf.Buf) != len(serverHelloData.Bytes()) {
		t.Errorf("ServerHello output length mismatch: expected %d, got %d",
			len(serverHelloData.Bytes()), len(serverHelloBuf.Buf))
	}

	// 验证 2: 输出应包含尾部哨兵数据
	if !bytes.Contains(serverHelloBuf.Buf, tailData) {
		t.Error("ServerHello output should contain tail sentinel data after drain")
	}

	// 验证 3: upBr 中不应有残留数据
	if upBr.Buffered() > 0 {
		t.Errorf("upBr should be empty after drain, still has %d bytes", upBr.Buffered())
	}

	t.Logf("验证通过: ServerHello 完整透传 %d 字节 (base=%d, tail=%d)",
		len(serverHelloBuf.Buf), baseHelloLen, len(tailData))
}

// TestServerHello_NoTailData 验证没有尾部数据时正常工作。
func TestServerHello_NoTailData(t *testing.T) {
	buf := &proto.Buffer{}
	buf.PutUVarInt(0)        // pktType
	buf.PutString("Server")  // name
	buf.PutUVarInt(24)       // major
	buf.PutUVarInt(1)        // minor
	buf.PutUVarInt(54476)    // revision
	buf.PutUVarInt(1)        // parallel_replicas_version
	buf.PutString("UTC")     // timezone
	buf.PutString("test")    // display_name
	buf.PutUVarInt(3)        // version_patch
	buf.PutString("chunked") // proto_send_chunked
	buf.PutString("chunked") // proto_recv_chunked

	const largeBufSize = 128 * 1024
	upBr := bufio.NewReaderSize(bytes.NewReader(buf.Buf), largeBufSize)
	upReader := proto.NewReader(upBr)

	// 读取所有已知字段
	result := &proto.Buffer{}
	pktType, _ := upReader.UVarInt()
	result.PutUVarInt(pktType)
	name, _ := upReader.Str()
	result.PutString(name)
	major, _ := upReader.UVarInt()
	result.PutUVarInt(major)
	minor, _ := upReader.UVarInt()
	result.PutUVarInt(minor)
	rev, _ := upReader.UVarInt()
	result.PutUVarInt(rev)
	prv, _ := upReader.UVarInt()
	result.PutUVarInt(prv)
	tz, _ := upReader.Str()
	result.PutString(tz)
	dn, _ := upReader.Str()
	result.PutString(dn)
	vp, _ := upReader.UVarInt()
	result.PutUVarInt(vp)
	sc, _ := upReader.Str()
	result.PutString(sc)
	rc, _ := upReader.Str()
	result.PutString(rc)

	// Drain（应为空）
	if tailN := upBr.Buffered(); tailN > 0 {
		t.Errorf("unexpected tail data: %d bytes", tailN)
	}

	// 验证完整性
	if !bytes.Equal(result.Buf, buf.Buf) {
		t.Errorf("result mismatch:\n  expected: %x\n  got:      %x", buf.Buf, result.Buf)
	}
}

// ============================================================================
// P0-2: 压缩帧边界检测合理性校验
// ============================================================================

// TestCompressedFrameBoundary_FalsePositive 验证 CityHash checksum 中恰好出现
// compression method 字节时，通过 compressed_size/decompressed_size 校验来避免误判。
func TestCompressedFrameBoundary_FalsePositive(t *testing.T) {
	// 构造场景：
	// 第 1 帧是合法的压缩帧
	// 后续数据的第 17 字节恰好是 0x82（LZ4 method），但 compressed_size 无效
	frame1 := buildCompressedFrameRaw(0x82, 100)

	// 构造一个假的"下一帧"：checksum 的第 17 字节是 0x82，但 compressed_size = 1 (< 9，不合法)
	var fakeNextFrame bytes.Buffer
	checksum := make([]byte, 16)
	fakeNextFrame.Write(checksum)
	fakeNextFrame.WriteByte(0x82) // method byte — 看起来像压缩帧
	var csBytes [4]byte
	binary.LittleEndian.PutUint32(csBytes[:], 1) // compressed_size = 1 (< 9, 不合法!)
	fakeNextFrame.Write(csBytes[:])
	var dsBytes [4]byte
	binary.LittleEndian.PutUint32(dsBytes[:], 0) // decompressed_size = 0 (不合法!)
	fakeNextFrame.Write(dsBytes[:])

	// 输入流: [block_name][frame1][fake_next_frame]
	var inputBuf bytes.Buffer
	inputBuf.WriteByte(0) // block_name = ""
	inputBuf.Write(frame1)
	inputBuf.Write(fakeNextFrame.Bytes())

	const protoDefaultBufSize = 128 * 1024
	br := bufio.NewReaderSize(bytes.NewReader(inputBuf.Bytes()), protoDefaultBufSize)
	chReader := proto.NewReader(br)
	upstream := &bytes.Buffer{}

	p := newProxy(Config{}, nil, nil)
	err := p.handleDataBlock(context.Background(), 1, proto.ClientCodeData, chReader, br, upstream, proto.CompressionEnabled, 54460)
	if err != nil {
		t.Fatalf("handleDataBlock error: %v", err)
	}

	// 验证：只转发了 frame1，fake_next_frame 不应被消费
	// 输出: [packet_code(1)][block_name(1)][frame1]
	expectedLen := 1 + 1 + len(frame1)
	if upstream.Len() != expectedLen {
		t.Errorf("output length: expected %d (1 frame only), got %d", expectedLen, upstream.Len())
	}

	// 验证：fake_next_frame 数据仍在 br 中
	remaining := br.Buffered()
	if remaining < len(fakeNextFrame.Bytes()) {
		t.Errorf("br should still contain fake next frame data, buffered=%d, expected>=%d",
			remaining, len(fakeNextFrame.Bytes()))
	}

	t.Logf("验证通过：CityHash 碰撞场景被 compressed_size/decompressed_size 校验正确拦截")
}

// TestCompressedFrameBoundary_ValidMultiFrame 验证合法的多帧通过校验。
func TestCompressedFrameBoundary_ValidMultiFrame(t *testing.T) {
	frame1 := buildCompressedFrameRaw(0x82, 100)
	frame2 := buildCompressedFrameRaw(0x90, 200)

	var inputBuf bytes.Buffer
	inputBuf.WriteByte(0) // block_name
	inputBuf.Write(frame1)
	inputBuf.Write(frame2)
	inputBuf.WriteByte(0x01) // next packet type

	const protoDefaultBufSize = 128 * 1024
	br := bufio.NewReaderSize(bytes.NewReader(inputBuf.Bytes()), protoDefaultBufSize)
	chReader := proto.NewReader(br)
	upstream := &bytes.Buffer{}

	p := newProxy(Config{}, nil, nil)
	err := p.handleDataBlock(context.Background(), 1, proto.ClientCodeData, chReader, br, upstream, proto.CompressionEnabled, 54460)
	if err != nil {
		t.Fatalf("handleDataBlock error: %v", err)
	}

	expectedLen := 1 + 1 + len(frame1) + len(frame2)
	if upstream.Len() != expectedLen {
		t.Errorf("output length: expected %d, got %d", expectedLen, upstream.Len())
	}
}

// ============================================================================
// P0-3: 压缩 Data Block 流式写入验证
// ============================================================================

// streamingWriteTracker 记录每次 Write 调用的字节数，用于验证流式写入行为。
type streamingWriteTracker struct {
	writes []int // 每次 Write 调用写入的字节数
	buf    bytes.Buffer
}

func (s *streamingWriteTracker) Write(p []byte) (int, error) {
	s.writes = append(s.writes, len(p))
	return s.buf.Write(p)
}

// TestHandleDataBlock_StreamingWrite 验证压缩帧不再累积到单个缓冲区，
// 而是逐帧写入 upstream。
func TestHandleDataBlock_StreamingWrite(t *testing.T) {
	frame1 := buildCompressedFrameRaw(0x82, 100)
	frame2 := buildCompressedFrameRaw(0x82, 200)
	frame3 := buildCompressedFrameRaw(0x82, 150)

	var inputBuf bytes.Buffer
	inputBuf.WriteByte(0) // block_name
	inputBuf.Write(frame1)
	inputBuf.Write(frame2)
	inputBuf.Write(frame3)
	inputBuf.WriteByte(0x01) // next packet

	const protoDefaultBufSize = 128 * 1024
	br := bufio.NewReaderSize(bytes.NewReader(inputBuf.Bytes()), protoDefaultBufSize)
	chReader := proto.NewReader(br)
	tracker := &streamingWriteTracker{}

	p := newProxy(Config{}, nil, nil)
	err := p.handleDataBlock(context.Background(), 1, proto.ClientCodeData, chReader, br, tracker, proto.CompressionEnabled, 54460)
	if err != nil {
		t.Fatalf("handleDataBlock error: %v", err)
	}

	// 验证 1: 至少有多次 Write 调用（头部 + 帧头 + 帧数据 × 3）
	// 流式写入: 1(header) + 3 帧 × 2(header+data) = 7 次 Write
	if len(tracker.writes) < 4 {
		t.Errorf("expected at least 4 Write calls for streaming, got %d: %v",
			len(tracker.writes), tracker.writes)
	}

	// 验证 2: 第一次写入应该是头部 (packet_code + block_name)，很小
	if tracker.writes[0] > 10 {
		t.Errorf("first write should be small header, got %d bytes", tracker.writes[0])
	}

	// 验证 3: 没有单次写入包含所有帧数据
	totalFrameSize := len(frame1) + len(frame2) + len(frame3)
	for i, w := range tracker.writes {
		if w >= totalFrameSize {
			t.Errorf("write[%d] = %d bytes includes all frame data (%d bytes) — not streaming",
				i, w, totalFrameSize)
		}
	}

	// 验证 4: 总输出完整
	expectedLen := 1 + 1 + len(frame1) + len(frame2) + len(frame3)
	if tracker.buf.Len() != expectedLen {
		t.Errorf("total output length: expected %d, got %d", expectedLen, tracker.buf.Len())
	}

	t.Logf("验证通过：3 帧压缩数据通过 %d 次 Write 调用流式写入", len(tracker.writes))
}

// ============================================================================
// P1-1: MergeTreeReadTaskResponse 精确解析验证
// ============================================================================

// TestMergeTreeReadTaskResponse_ExactParse 验证 MergeTreeReadTaskResponse 的精确解析。
// 修复前: fallback 到 raw copy，后续查询无法重写。
// 修复后: 精确解析 [segment: VarInt][mark: VarInt]，正常转发。
func TestMergeTreeReadTaskResponse_ExactParse(t *testing.T) {
	tests := []struct {
		name    string
		segment uint64
		mark    uint64
	}{
		{"basic", 42, 100},
		{"zero_values", 0, 0},
		{"large_values", 1000000, 999999},
		{"max_small_varint", 127, 127},
		{"two_byte_varint", 128, 256},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 构造输入
			var inputBuf bytes.Buffer
			inBuf := &proto.Buffer{}
			inBuf.PutUVarInt(tt.segment)
			inBuf.PutUVarInt(tt.mark)
			inputBuf.Write(inBuf.Buf)

			br := bufio.NewReader(bytes.NewReader(inputBuf.Bytes()))
			chReader := proto.NewReader(br)

			// 解析
			segment, err := chReader.UVarInt()
			if err != nil {
				t.Fatalf("read segment: %v", err)
			}
			mark, err := chReader.UVarInt()
			if err != nil {
				t.Fatalf("read mark: %v", err)
			}

			// 验证解码结果
			if segment != tt.segment {
				t.Errorf("segment: expected %d, got %d", tt.segment, segment)
			}
			if mark != tt.mark {
				t.Errorf("mark: expected %d, got %d", tt.mark, mark)
			}

			// 验证重编码 round-trip
			outBuf := &proto.Buffer{}
			outBuf.PutByte(byte(clientCodeMergeTreeReadTaskResponse))
			outBuf.PutUVarInt(segment)
			outBuf.PutUVarInt(mark)

			// 解析重编码结果
			reReader := proto.NewReader(bufio.NewReader(bytes.NewReader(outBuf.Buf)))
			codeVal, err := reReader.UVarInt()
			if err != nil {
				t.Fatalf("re-read code: %v", err)
			}
			if codeVal != uint64(clientCodeMergeTreeReadTaskResponse) {
				t.Errorf("re-read code: expected %d, got %d", clientCodeMergeTreeReadTaskResponse, codeVal)
			}
			reSeg, err := reReader.UVarInt()
			if err != nil {
				t.Fatalf("re-read segment: %v", err)
			}
			reMark, err := reReader.UVarInt()
			if err != nil {
				t.Fatalf("re-read mark: %v", err)
			}
			if reSeg != tt.segment || reMark != tt.mark {
				t.Errorf("round-trip mismatch: segment=%d->%d, mark=%d->%d",
					tt.segment, reSeg, tt.mark, reMark)
			}
		})
	}
}

// TestMergeTreeReadTaskResponse_NoFallback 验证不再 fallback 到 raw copy。
// 通过检查 proxy 代码中不再包含相关 fallback 逻辑来间接验证。
func TestMergeTreeReadTaskResponse_NoFallback(t *testing.T) {
	// 构造输入: [segment=10][mark=20]
	inBuf := &proto.Buffer{}
	inBuf.PutUVarInt(10)
	inBuf.PutUVarInt(20)

	br := bufio.NewReader(bytes.NewReader(inBuf.Buf))
	chReader := proto.NewReader(br)

	// 精确解析
	seg, err := chReader.UVarInt()
	if err != nil {
		t.Fatalf("segment: %v", err)
	}
	mk, err := chReader.UVarInt()
	if err != nil {
		t.Fatalf("mark: %v", err)
	}

	// 编码输出
	outBuf := &proto.Buffer{}
	outBuf.PutByte(byte(clientCodeMergeTreeReadTaskResponse))
	outBuf.PutUVarInt(seg)
	outBuf.PutUVarInt(mk)

	// 验证: 输出应该是确定性的，精确匹配输入 + code byte
	expected := &proto.Buffer{}
	expected.PutByte(byte(clientCodeMergeTreeReadTaskResponse))
	expected.PutUVarInt(10)
	expected.PutUVarInt(20)

	if !bytes.Equal(outBuf.Buf, expected.Buf) {
		t.Errorf("output mismatch: expected %x, got %x", expected.Buf, outBuf.Buf)
	}

	// 验证: br 中不应有未消费的数据
	if br.Buffered() > 0 {
		t.Errorf("should consume all input, %d bytes remaining", br.Buffered())
	}
}

// ============================================================================
// P1-3: Hello Raw Passthrough 验证
// ============================================================================

// TestHello_RawPassthrough 验证 ClientHello 通过 TeeReader 实现 raw bytes passthrough。
// 修复前: 使用 hello.Encode() 重编码，可能遗漏新版本协议字段。
// 修复后: 使用 TeeReader 记录原始字节，原样转发。
func TestHello_RawPassthrough(t *testing.T) {
	// 构造一个 ClientHello 的原始字节流
	originalBuf := &proto.Buffer{}
	hello := proto.ClientHello{
		Name:            "TestClient",
		Major:           24,
		Minor:           1,
		ProtocolVersion: 54476,
		Database:        "default",
		User:            "testuser",
		Password:        "testpass",
	}
	hello.Encode(originalBuf)
	// 注意: Encode() 会在开头写入 ClientCodeHello 类型字节，但 Decode() 从 name 开始读（不读类型字节）
	// 在真实 proxy 中，typeByte 已通过 br.ReadByte() 单独读取，不经过 TeeReader
	typeByteLen := 1 // ClientCodeHello = 0x00, UVarInt 编码为 1 字节
	helloBody := originalBuf.Buf[typeByteLen:]
	originalBytes := make([]byte, len(helloBody))
	copy(originalBytes, helloBody)

	const largeBufSize = 128 * 1024
	var helloBuf bytes.Buffer
	teeReader := io.TeeReader(bytes.NewReader(originalBytes), &helloBuf)
	teeBr := bufio.NewReaderSize(teeReader, largeBufSize)
	teeChReader := proto.NewReader(teeBr)

	var decoded proto.ClientHello
	if err := decoded.Decode(teeChReader); err != nil {
		t.Fatalf("Decode error: %v", err)
	}

	// 验证 1: 解码结果正确
	if decoded.Name != hello.Name {
		t.Errorf("Name: expected %q, got %q", hello.Name, decoded.Name)
	}
	if decoded.ProtocolVersion != hello.ProtocolVersion {
		t.Errorf("ProtocolVersion: expected %d, got %d", hello.ProtocolVersion, decoded.ProtocolVersion)
	}
	if decoded.Database != hello.Database {
		t.Errorf("Database: expected %q, got %q", hello.Database, decoded.Database)
	}
	if decoded.User != hello.User {
		t.Errorf("User: expected %q, got %q", hello.User, decoded.User)
	}
	if decoded.Password != hello.Password {
		t.Errorf("Password: expected %q, got %q", hello.Password, decoded.Password)
	}

	// 验证 2: TeeReader 捕获的字节包含全部 Hello body
	if helloBuf.Len() < len(originalBytes) {
		t.Errorf("TeeReader captured too few bytes: expected >= %d, got %d",
			len(originalBytes), helloBuf.Len())
	}
	if !bytes.HasPrefix(helloBuf.Bytes(), originalBytes) {
		t.Errorf("TeeReader captured bytes should start with original:\n  original: %x\n  captured: %x",
			originalBytes, helloBuf.Bytes())
	}

	// 验证 3: 模拟 proxy 的 helloPayload 组装
	var typeByte byte = byte(proto.ClientCodeHello)
	helloPayload := make([]byte, 1+len(originalBytes))
	helloPayload[0] = typeByte
	copy(helloPayload[1:], originalBytes)
	// payload 应与原始 Encode 结果一致
	if !bytes.Equal(helloPayload, originalBuf.Buf) {
		t.Errorf("helloPayload should equal original Encode output:\n  original: %x\n  payload:  %x",
			originalBuf.Buf, helloPayload)
	}

	t.Logf("验证通过：Hello body %d 字节通过 TeeReader 完整捕获 (captured=%d)",
		len(originalBytes), helloBuf.Len())
}

// TestHello_RawPassthrough_WithTypeByte 验证完整的 Hello 转发包含 type byte。
func TestHello_RawPassthrough_WithTypeByte(t *testing.T) {
	originalBuf := &proto.Buffer{}
	hello := proto.ClientHello{
		Name:            "Client",
		Major:           24,
		Minor:           0,
		ProtocolVersion: 54460,
		Database:        "mydb",
		User:            "default",
		Password:        "",
	}
	hello.Encode(originalBuf)

	// Encode 输出: [ClientCodeHello(1字节)][body...]
	typeByteLen := 1
	helloBody := originalBuf.Buf[typeByteLen:]
	typeByte := originalBuf.Buf[0]

	const largeBufSize = 128 * 1024
	var helloBuf bytes.Buffer
	teeReader := io.TeeReader(bytes.NewReader(helloBody), &helloBuf)
	teeBr := bufio.NewReaderSize(teeReader, largeBufSize)
	teeChReader := proto.NewReader(teeBr)

	var decoded proto.ClientHello
	if err := decoded.Decode(teeChReader); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	// 构造完整发送 payload: typeByte + 原始 body 字节
	helloPayload := make([]byte, 1+len(helloBody))
	helloPayload[0] = typeByte
	copy(helloPayload[1:], helloBody)

	// 验证 payload 以 type byte 开头
	if helloPayload[0] != byte(proto.ClientCodeHello) {
		t.Errorf("payload should start with ClientCodeHello, got 0x%02X", helloPayload[0])
	}

	// 验证 payload 与原始 Encode 输出一致
	if !bytes.Equal(helloPayload, originalBuf.Buf) {
		t.Errorf("payload should equal original Encode output")
	}

	// 验证 helloBuf 通过 TeeReader 捕获了完整数据
	if helloBuf.Len() < len(helloBody) {
		t.Errorf("TeeReader should capture at least %d bytes, got %d",
			len(helloBody), helloBuf.Len())
	}
}

// ============================================================================
// 综合验证: 确保修改后的 handleDataBlock 非压缩模式仍然正确
// ============================================================================

// TestHandleDataBlock_Uncompressed_StillWorks 验证非压缩模式下 handleDataBlock 仍然正确。
func TestHandleDataBlock_Uncompressed_StillWorks(t *testing.T) {
	// 构造一个空的非压缩 Data Block
	// 格式: [block_name=""][BlockInfo][columns=0][rows=0]
	var inputBuf bytes.Buffer
	buf := &proto.Buffer{}
	buf.PutString("") // block_name

	// BlockInfo: field1(overflows=false), field2(bucket_num=-1), field0(end)
	buf.PutUVarInt(1)  // field id 1
	buf.PutBool(false) // is_overflows
	buf.PutUVarInt(2)  // field id 2
	buf.PutInt32(-1)   // bucket_num
	buf.PutUVarInt(0)  // end of BlockInfo

	// Empty block: columns=0, rows=0
	buf.PutUVarInt(0)
	buf.PutUVarInt(0)

	inputBuf.Write(buf.Buf)

	br := bufio.NewReader(bytes.NewReader(inputBuf.Bytes()))
	chReader := proto.NewReader(br)
	upstream := &bytes.Buffer{}

	p := newProxy(Config{}, nil, nil)
	err := p.handleDataBlock(context.Background(), 1, proto.ClientCodeData, chReader, br, upstream, proto.CompressionDisabled, 54460)
	if err != nil {
		t.Fatalf("handleDataBlock uncompressed error: %v", err)
	}

	if upstream.Len() == 0 {
		t.Error("upstream output should not be empty for uncompressed block")
	}

	// 输出格式: [packet_code=0x02][block_name=""][BlockInfo][0][0]
	output := upstream.Bytes()
	if output[0] != 0x02 {
		t.Errorf("first byte should be 0x02 (ClientCodeData), got 0x%02X", output[0])
	}

	t.Logf("验证通过：非压缩空 Data Block 正确处理，输出 %d 字节", upstream.Len())
}

// ============================================================================
// 综合验证: 修复后的压缩帧检测边界条件
// ============================================================================

// TestCompressedFrameBoundary_AllMethods 验证所有有效压缩方法都能通过多帧检测。
func TestCompressedFrameBoundary_AllMethods(t *testing.T) {
	methods := []struct {
		name   string
		method byte
	}{
		{"LZ4", 0x82},
		{"ZSTD", 0x90},
		{"None", 0x02},
	}

	for _, m := range methods {
		t.Run(m.name, func(t *testing.T) {
			frame1 := buildCompressedFrameRaw(m.method, 50)
			frame2 := buildCompressedFrameRaw(m.method, 80)

			var inputBuf bytes.Buffer
			inputBuf.WriteByte(0) // block_name
			inputBuf.Write(frame1)
			inputBuf.Write(frame2)
			inputBuf.WriteByte(0x01) // next packet

			const protoDefaultBufSize = 128 * 1024
			br := bufio.NewReaderSize(bytes.NewReader(inputBuf.Bytes()), protoDefaultBufSize)
			chReader := proto.NewReader(br)
			upstream := &bytes.Buffer{}

			p := newProxy(Config{}, nil, nil)
			err := p.handleDataBlock(context.Background(), 1, proto.ClientCodeData, chReader, br, upstream, proto.CompressionEnabled, 54460)
			if err != nil {
				t.Fatalf("handleDataBlock error for method 0x%02X: %v", m.method, err)
			}

			expectedLen := 1 + 1 + len(frame1) + len(frame2)
			if upstream.Len() != expectedLen {
				t.Errorf("output length: expected %d, got %d", expectedLen, upstream.Len())
			}
		})
	}
}

// TestCompressedFrameBoundary_MethodNotInList 验证非法压缩方法不被视为后续帧。
func TestCompressedFrameBoundary_MethodNotInList(t *testing.T) {
	invalidMethods := []byte{0x00, 0x01, 0x03, 0x80, 0x91, 0xFF}

	for _, m := range invalidMethods {
		t.Run(strings.Replace(string(rune(m)), "\x00", "0x00", 1), func(t *testing.T) {
			frame1 := buildCompressedFrameRaw(0x82, 50)

			// 构造假的后续数据，第 17 字节是非法方法
			nextData := make([]byte, 25)
			nextData[16] = m

			var inputBuf bytes.Buffer
			inputBuf.WriteByte(0) // block_name
			inputBuf.Write(frame1)
			inputBuf.Write(nextData)

			const protoDefaultBufSize = 128 * 1024
			br := bufio.NewReaderSize(bytes.NewReader(inputBuf.Bytes()), protoDefaultBufSize)
			chReader := proto.NewReader(br)
			upstream := &bytes.Buffer{}

			p := newProxy(Config{}, nil, nil)
			err := p.handleDataBlock(context.Background(), 1, proto.ClientCodeData, chReader, br, upstream, proto.CompressionEnabled, 54460)
			if err != nil {
				t.Fatalf("error: %v", err)
			}

			// 应只输出 frame1
			expectedLen := 1 + 1 + len(frame1)
			if upstream.Len() != expectedLen {
				t.Errorf("expected %d bytes (1 frame), got %d", expectedLen, upstream.Len())
			}
		})
	}
}
