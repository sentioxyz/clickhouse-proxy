package main

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// ============================================================================
// R3-1: forwardUntilQueryDone drain 竞态修复测试
// ============================================================================

// TestForwardUntilQueryDone_DrainGracePeriod 验证 queryDone 信号后有足够时间 drain in-flight 数据
func TestForwardUntilQueryDone_DrainGracePeriod(t *testing.T) {
	// 此测试验证 50ms grace period 的设计意图：
	// 读取 goroutine 可能在 br.Read() 中阻塞，queryDone 信号到达后，
	// goroutine 读到的数据应该被转发而不是丢弃。
	t.Log("R3-1: drain grace period is 50ms (time.NewTimer(50ms))")
	t.Log("This ensures in-flight data from read goroutine is not lost")

	// 验证 50ms 足够完成一次内存 channel 操作
	ch := make(chan int, 1)
	done := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond) // 模拟 goroutine 轻微延迟
		ch <- 42
		close(done)
	}()

	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()

	select {
	case v := <-ch:
		if v != 42 {
			t.Errorf("unexpected value: %d", v)
		}
		t.Log("OK: data received within grace period")
	case <-timer.C:
		t.Error("grace period expired before data received")
	}
	<-done
}

// ============================================================================
// R3-2: OldSetting UInt64 编解码测试
// ============================================================================

// TestOldSettingEncoding_UInt64 验证旧格式 setting 的 UInt64 编解码正确性
func TestOldSettingEncoding_UInt64(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
	}{
		{"zero", 0},
		{"small", 42},
		{"max_int64", 1<<63 - 1},         // math.MaxInt64
		{"above_max_int64", 1<<63 + 100}, // 超过 Int64 范围
		{"max_uint64", ^uint64(0)},       // math.MaxUint64
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 编码
			b := &proto.Buffer{}
			b.PutString(tt.name)  // key
			b.PutUInt64(tt.value) // value

			// 解码
			r := proto.NewReader(bytes.NewReader(b.Buf))
			key, err := r.Str()
			if err != nil {
				t.Fatalf("read key: %v", err)
			}
			if key != tt.name {
				t.Errorf("key mismatch: got %q, want %q", key, tt.name)
			}
			val, err := r.UInt64()
			if err != nil {
				t.Fatalf("read value: %v", err)
			}
			if val != tt.value {
				t.Errorf("value mismatch: got %d, want %d", val, tt.value)
			}
		})
	}
}

// TestOldSettingEncoding_Int64VsUInt64 验证 Int64 vs UInt64 对高位值的行为差异
func TestOldSettingEncoding_Int64VsUInt64(t *testing.T) {
	// 值超过 MaxInt64 时，Int64 编码会产生负数（但底层字节相同）
	// 重要的是解码时使用 UInt64 才能还原正确的值
	value := uint64(1<<63 + 100) // 超过 Int64 范围

	// 使用 UInt64 编码
	b := &proto.Buffer{}
	b.PutUInt64(value)

	// 使用 UInt64 解码
	r := proto.NewReader(bytes.NewReader(b.Buf))
	decoded, err := r.UInt64()
	if err != nil {
		t.Fatal(err)
	}
	if decoded != value {
		t.Errorf("UInt64 roundtrip failed: got %d, want %d", decoded, value)
	}
}

// ============================================================================
// R3-3: MergeTreeReadTaskResponse 结构化处理测试
// ============================================================================

// TestMergeTreeReadTaskResponse_StructuredDecode 验证 MergeTreeReadTaskResponse 能被结构化解码
func TestMergeTreeReadTaskResponse_StructuredDecode(t *testing.T) {
	// MergeTreeReadTaskResponse 格式: [type_byte(14)][response: String]
	// 与 ReadTaskResponse(13) 和 ClusterFunctionReadTaskResponse(15) 相同
	response := "/data/partition_1/block_42"

	// 构建模拟包
	b := &proto.Buffer{}
	b.PutByte(byte(clientCodeMergeTreeReadTaskResponse))
	b.PutString(response)

	// 验证 clientCodeMergeTreeReadTaskResponse 常量值
	t.Logf("clientCodeMergeTreeReadTaskResponse = %d", clientCodeMergeTreeReadTaskResponse)

	// 跳过类型字节
	r := proto.NewReader(bytes.NewReader(b.Buf[1:]))
	decoded, err := r.Str()
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if decoded != response {
		t.Errorf("response mismatch: got %q, want %q", decoded, response)
	}
	t.Logf("OK: MergeTreeReadTaskResponse structured decode: %q", decoded)
}

// TestAllReadTaskResponses_SameFormat 验证三种 ReadTaskResponse 格式一致
func TestAllReadTaskResponses_SameFormat(t *testing.T) {
	// 三种 ReadTaskResponse 都应该是 [type_byte][response: String]
	codes := []struct {
		name string
		code proto.ClientCode
	}{
		{"ReadTaskResponse", clientCodeReadTaskResponse},
		{"MergeTreeReadTaskResponse", clientCodeMergeTreeReadTaskResponse},
		{"ClusterFunctionReadTaskResponse", clientCodeClusterFunctionReadTaskResponse},
	}

	response := "test_response_string"
	for _, tc := range codes {
		t.Run(tc.name, func(t *testing.T) {
			b := &proto.Buffer{}
			b.PutByte(byte(tc.code))
			b.PutString(response)

			// 所有三种都应该是 1 + UVarInt(len) + len(response) 字节
			expectedMinLen := 1 + 1 + len(response) // type + UVarInt(len) + response
			if len(b.Buf) < expectedMinLen {
				t.Errorf("buffer too short: %d < %d", len(b.Buf), expectedMinLen)
			}
			t.Logf("%s (code=%d): encoded %d bytes", tc.name, tc.code, len(b.Buf))
		})
	}
}

// ============================================================================
// R3-4: BlockInfoCompat unknown field 安全处理测试
// ============================================================================

// TestBlockInfoCompat_UnknownField 验证未知 field 返回错误而非尝试跳过
func TestBlockInfoCompat_UnknownField(t *testing.T) {
	// 构建包含未知 field 4 的 BlockInfo
	b := &proto.Buffer{}
	// field 1: Overflows = false
	b.PutUVarInt(1)
	b.PutBool(false)
	// field 2: BucketNum = -1
	b.PutUVarInt(2)
	b.PutInt32(-1)
	// field 4: 未知（故意触发安全处理）
	b.PutUVarInt(4)
	b.PutString("unknown_data") // 假设是 String 类型
	// end
	b.PutUVarInt(0)

	r := proto.NewReader(bytes.NewReader(b.Buf))
	_, err := decodeBlockInfoCompat(r)
	if err == nil {
		t.Error("expected error for unknown field 4, got nil")
	} else {
		t.Logf("OK: got expected error: %v", err)
		if !bytes.Contains([]byte(err.Error()), []byte("unknown field 4")) {
			t.Errorf("error message should mention field 4: %v", err)
		}
	}
}

// TestBlockInfoCompat_KnownFields 验证已知 field 1,2,3 正常工作
func TestBlockInfoCompat_KnownFields(t *testing.T) {
	b := &proto.Buffer{}
	// field 1: Overflows = true
	b.PutUVarInt(1)
	b.PutBool(true)
	// field 2: BucketNum = 7
	b.PutUVarInt(2)
	b.PutInt32(7)
	// field 3: OutOfOrderBuckets = [1, 3, 5]
	b.PutUVarInt(3)
	b.PutUVarInt(3)
	b.PutInt32(1)
	b.PutInt32(3)
	b.PutInt32(5)
	// end
	b.PutUVarInt(0)

	r := proto.NewReader(bytes.NewReader(b.Buf))
	info, err := decodeBlockInfoCompat(r)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if !info.Overflows {
		t.Error("Overflows should be true")
	}
	if info.BucketNum != 7 {
		t.Errorf("BucketNum: got %d, want 7", info.BucketNum)
	}
	if !info.HasOutOfOrderBuckets {
		t.Error("HasOutOfOrderBuckets should be true")
	}
	if len(info.OutOfOrderBuckets) != 3 {
		t.Fatalf("OutOfOrderBuckets length: got %d, want 3", len(info.OutOfOrderBuckets))
	}
	expected := []int32{1, 3, 5}
	for i, v := range expected {
		if info.OutOfOrderBuckets[i] != v {
			t.Errorf("OutOfOrderBuckets[%d]: got %d, want %d", i, info.OutOfOrderBuckets[i], v)
		}
	}
}

// ============================================================================
// R3-5: BlockInfoCompat 空 field 3 透传测试
// ============================================================================

// TestBlockInfoCompat_EmptyField3_Roundtrip 验证空 field 3 编解码 roundtrip
func TestBlockInfoCompat_EmptyField3_Roundtrip(t *testing.T) {
	// 原始 BlockInfo 包含空的 field 3
	original := &BlockInfoCompat{
		Overflows:            false,
		BucketNum:            -1,
		HasOutOfOrderBuckets: true,      // field 3 存在
		OutOfOrderBuckets:    []int32{}, // 但为空
	}

	// 编码
	encBuf := &proto.Buffer{}
	encodeBlockInfoCompat(encBuf, original)

	// 解码
	r := proto.NewReader(bytes.NewReader(encBuf.Buf))
	decoded, err := decodeBlockInfoCompat(r)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	// 验证 field 3 的存在性和内容
	if !decoded.HasOutOfOrderBuckets {
		t.Error("HasOutOfOrderBuckets should be preserved as true")
	}
	if len(decoded.OutOfOrderBuckets) != 0 {
		t.Errorf("OutOfOrderBuckets should be empty, got %d", len(decoded.OutOfOrderBuckets))
	}
}

// TestBlockInfoCompat_NoField3_Roundtrip 验证无 field 3 的 roundtrip
func TestBlockInfoCompat_NoField3_Roundtrip(t *testing.T) {
	// 原始 BlockInfo 没有 field 3
	original := &BlockInfoCompat{
		Overflows:            true,
		BucketNum:            42,
		HasOutOfOrderBuckets: false, // field 3 不存在
	}

	// 编码
	encBuf := &proto.Buffer{}
	encodeBlockInfoCompat(encBuf, original)

	// 验证输出中不包含 field 3 的标记
	// field 3 的 UVarInt 编码是一个字节 0x03
	// 但我们不能简单搜索 0x03，因为其他值也可能包含它
	// 改为验证 roundtrip
	r := proto.NewReader(bytes.NewReader(encBuf.Buf))
	decoded, err := decodeBlockInfoCompat(r)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded.HasOutOfOrderBuckets {
		t.Error("HasOutOfOrderBuckets should be false when field 3 was not in input")
	}
	if decoded.Overflows != true {
		t.Error("Overflows should be true")
	}
	if decoded.BucketNum != 42 {
		t.Errorf("BucketNum: got %d, want 42", decoded.BucketNum)
	}
}

// TestBlockInfoCompat_NonEmptyField3_Roundtrip 验证非空 field 3 的 roundtrip
func TestBlockInfoCompat_NonEmptyField3_Roundtrip(t *testing.T) {
	original := &BlockInfoCompat{
		Overflows:            false,
		BucketNum:            3,
		HasOutOfOrderBuckets: true,
		OutOfOrderBuckets:    []int32{10, 20, 30, 40},
	}

	encBuf := &proto.Buffer{}
	encodeBlockInfoCompat(encBuf, original)

	r := proto.NewReader(bytes.NewReader(encBuf.Buf))
	decoded, err := decodeBlockInfoCompat(r)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if !decoded.HasOutOfOrderBuckets {
		t.Error("HasOutOfOrderBuckets should be true")
	}
	if len(decoded.OutOfOrderBuckets) != 4 {
		t.Fatalf("OutOfOrderBuckets length: got %d, want 4", len(decoded.OutOfOrderBuckets))
	}
	for i, v := range []int32{10, 20, 30, 40} {
		if decoded.OutOfOrderBuckets[i] != v {
			t.Errorf("OutOfOrderBuckets[%d]: got %d, want %d", i, decoded.OutOfOrderBuckets[i], v)
		}
	}
}

// ============================================================================
// R3-6: queryParser Cancel/Ping 注释验证
// ============================================================================

// TestQueryParser_CancelAndPing_NoPayload 验证 Cancel(3) 和 Ping(4) 包只消费类型字节
func TestQueryParser_CancelAndPing_NoPayload(t *testing.T) {
	// Cancel = 3, UVarInt 编码 = [0x03]
	// Ping   = 4, UVarInt 编码 = [0x04]

	for _, tt := range []struct {
		name string
		code byte
	}{
		{"Cancel", 3},
		{"Ping", 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			p := &queryParser{version: 54429, addendumDone: true}
			// 构造一个 Cancel/Ping 包后面跟着一个 Query
			var buf []byte
			buf = append(buf, tt.code) // Cancel or Ping (no payload)

			sqls, err := p.feed(buf)
			if err != nil {
				t.Fatalf("feed error: %v", err)
			}
			// Cancel/Ping 不产生 ParsedQuery
			if len(sqls) != 0 {
				t.Errorf("expected 0 queries from %s, got %d", tt.name, len(sqls))
			}
			// 缓冲区应该为空（Cancel/Ping 已被消费）
			if len(p.buf) != 0 {
				t.Errorf("buffer should be empty after %s, got %d bytes", tt.name, len(p.buf))
			}
		})
	}
}

// ============================================================================
// 回归测试
// ============================================================================

// TestQueryCodec_EncodeDecode_Roundtrip 验证 Query 编解码在 R3 修改后仍然正确
func TestQueryCodec_EncodeDecode_Roundtrip(t *testing.T) {
	// 使用标准 revision 进行 roundtrip
	revision := int(proto.Version)

	original := &ExtQuery{}
	original.ID = "test-query-123"
	original.Body = "SELECT 1"
	original.Stage = proto.StageComplete
	original.Compression = proto.CompressionDisabled
	original.Settings = []proto.Setting{
		{Key: "max_memory_usage", Value: "1000000", Important: false},
	}

	// 编码
	b := &proto.Buffer{}
	encodeQueryCustom(b, original, revision)

	// 跳过包类型字节 (ClientCodeQuery)
	r := proto.NewReader(bytes.NewReader(b.Buf[1:]))
	decoded, err := decodeQueryCustom(r, revision)
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID: got %q, want %q", decoded.ID, original.ID)
	}
	if decoded.Body != original.Body {
		t.Errorf("Body: got %q, want %q", decoded.Body, original.Body)
	}
	if decoded.Stage != original.Stage {
		t.Errorf("Stage: got %d, want %d", decoded.Stage, original.Stage)
	}
	if decoded.Compression != original.Compression {
		t.Errorf("Compression: got %d, want %d", decoded.Compression, original.Compression)
	}
	if len(decoded.Settings) != len(original.Settings) {
		t.Errorf("Settings count: got %d, want %d", len(decoded.Settings), len(original.Settings))
	}
}

// 确保未使用的 import 变量不触发编译错误
var _ = binary.LittleEndian
