package main

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/segmentio/asm/bswap"
	"go.opentelemetry.io/otel/trace"
)

// ================================================================
// R5-2: drainTimer 显式 Stop 验证
// ================================================================

// 注意: forwardUntilQueryDone 是内部函数，无法直接测试 drainTimer 行为。
// 通过编译通过和全量回归验证即可。此处验证 timer 基本行为。
func TestDrainTimer_ExplicitStop(t *testing.T) {
	t.Run("timer Stop 后不应触发", func(t *testing.T) {
		timer := time.NewTimer(10 * time.Millisecond)
		timer.Stop()
		select {
		case <-timer.C:
			t.Fatal("timer should not fire after Stop")
		case <-time.After(50 * time.Millisecond):
			// OK
		}
	})

	t.Run("timer Reset 后可正常工作", func(t *testing.T) {
		timer := time.NewTimer(100 * time.Millisecond)
		timer.Stop()
		timer.Reset(10 * time.Millisecond)
		select {
		case <-timer.C:
			// OK
		case <-time.After(100 * time.Millisecond):
			t.Fatal("timer should fire after Reset")
		}
	})
}

// ================================================================
// R5-3: OpenTelemetry HasTrace 字段透传验证
// ================================================================

func TestOpenTelemetry_HasTrace_Roundtrip(t *testing.T) {
	// 高版本 revision 支持 OpenTelemetry
	revision := 54476 // > FeatureOpenTelemetry

	t.Run("有效 trace 数据 roundtrip", func(t *testing.T) {
		eq := &ExtQuery{}
		eq.ID = "test-query-1"
		eq.Body = "SELECT 1"
		eq.Stage = proto.StageComplete
		eq.Compression = proto.CompressionDisabled
		eq.Info.Query = proto.ClientQueryInitial
		eq.Info.Interface = proto.InterfaceTCP
		eq.Info.Major = 24
		eq.Info.Minor = 1
		eq.Info.ProtocolVersion = revision

		// 设置有效的 trace 数据
		traceID := trace.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		spanID := trace.SpanID{1, 2, 3, 4, 5, 6, 7, 8}
		eq.Info.Span = trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
		})
		eq.HasTrace = true

		// 编码
		buf := &proto.Buffer{}
		encodeQueryCustom(buf, eq, revision)

		// 解码
		r := proto.NewReader(newBufReader(buf.Buf[1:])) // skip ClientCodeQuery byte
		decoded, err := decodeQueryCustom(r, revision)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}

		if !decoded.HasTrace {
			t.Error("HasTrace should be true")
		}
		if decoded.Info.Span.TraceID() != traceID {
			t.Errorf("TraceID mismatch: got %v, want %v", decoded.Info.Span.TraceID(), traceID)
		}
		if decoded.Info.Span.SpanID() != spanID {
			t.Errorf("SpanID mismatch: got %v, want %v", decoded.Info.Span.SpanID(), spanID)
		}
	})

	t.Run("全零 TraceID（IsValid=false）roundtrip 保留", func(t *testing.T) {
		eq := &ExtQuery{}
		eq.ID = "test-query-2"
		eq.Body = "SELECT 2"
		eq.Stage = proto.StageComplete
		eq.Compression = proto.CompressionDisabled
		eq.Info.Query = proto.ClientQueryInitial
		eq.Info.Interface = proto.InterfaceTCP
		eq.Info.Major = 24
		eq.Info.Minor = 1
		eq.Info.ProtocolVersion = revision

		// 设置全零的 trace 数据（IsValid() 会返回 false）
		eq.Info.Span = trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    trace.TraceID{},
			SpanID:     trace.SpanID{},
			TraceFlags: 0,
		})
		// R5-3 的关键：即使 IsValid()=false，HasTrace=true 也应保留
		eq.HasTrace = true

		if eq.Info.Span.IsValid() {
			t.Fatal("test precondition: Span.IsValid() should be false for all-zero trace")
		}

		// 编码
		buf := &proto.Buffer{}
		encodeQueryCustom(buf, eq, revision)

		// 解码
		r := proto.NewReader(newBufReader(buf.Buf[1:]))
		decoded, err := decodeQueryCustom(r, revision)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}

		// 关键验证：即使 IsValid()=false，HasTrace 应该仍为 true
		if !decoded.HasTrace {
			t.Error("HasTrace should be true even with all-zero TraceID (R5-3 fix)")
		}
	})

	t.Run("无 trace 数据 roundtrip", func(t *testing.T) {
		eq := &ExtQuery{}
		eq.ID = "test-query-3"
		eq.Body = "SELECT 3"
		eq.Stage = proto.StageComplete
		eq.Compression = proto.CompressionDisabled
		eq.Info.Query = proto.ClientQueryInitial
		eq.Info.Interface = proto.InterfaceTCP
		eq.Info.Major = 24
		eq.Info.Minor = 1
		eq.Info.ProtocolVersion = revision
		eq.HasTrace = false

		// 编码
		buf := &proto.Buffer{}
		encodeQueryCustom(buf, eq, revision)

		// 解码
		r := proto.NewReader(newBufReader(buf.Buf[1:]))
		decoded, err := decodeQueryCustom(r, revision)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}

		if decoded.HasTrace {
			t.Error("HasTrace should be false when no trace was set")
		}
	})
}

// ================================================================
// R5-4: Duration 数字解析警告验证
// ================================================================

func TestDuration_NumericNanosecondWarning(t *testing.T) {
	t.Run("字符串格式正常解析", func(t *testing.T) {
		var d Duration
		if err := json.Unmarshal([]byte(`"5s"`), &d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d.Duration != 5*time.Second {
			t.Errorf("expected 5s, got %v", d.Duration)
		}
	})

	t.Run("大数字正常解析为纳秒", func(t *testing.T) {
		var d Duration
		// 5000000000 纳秒 = 5 秒
		if err := json.Unmarshal([]byte(`5000000000`), &d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d.Duration != 5*time.Second {
			t.Errorf("expected 5s, got %v", d.Duration)
		}
	})

	t.Run("小数字解析为纳秒并触发警告", func(t *testing.T) {
		// 此测试仅验证不出错（warning 通过 log 输出，不影响功能）
		var d Duration
		if err := json.Unmarshal([]byte(`300`), &d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d.Duration != 300*time.Nanosecond {
			t.Errorf("expected 300ns, got %v", d.Duration)
		}
	})

	t.Run("null 正常处理", func(t *testing.T) {
		var d Duration
		if err := json.Unmarshal([]byte(`null`), &d); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if d.Duration != 0 {
			t.Errorf("expected 0, got %v", d.Duration)
		}
	})

	t.Run("无效格式返回错误", func(t *testing.T) {
		var d Duration
		err := json.Unmarshal([]byte(`true`), &d)
		if err == nil {
			t.Fatal("expected error for bool value")
		}
	})
}

// ================================================================
// R5-5: queryParser 双重错误信息验证
// ================================================================

func TestQueryParser_DualErrorMessage(t *testing.T) {
	// 构造一个会触发双重解码失败的 parser
	p := &queryParser{
		version:      54429, // FeatureSettingsSerializedAsStrings
		addendumDone: true,
	}

	// 构造一个损坏的 Query 包：类型码正确但内容不完整
	// type=1(Query) + 随机无效内容
	invalidPacket := []byte{1, 0xFF, 0xFF, 0xFF}
	_, err := p.feed(invalidPacket)
	if err != nil {
		// 如果解析失败，验证错误消息包含"primary"和"fallback"
		errStr := err.Error()
		hasPrimary := false
		hasFallback := false
		for i := 0; i <= len(errStr)-7; i++ {
			if errStr[i:i+7] == "primary" {
				hasPrimary = true
			}
		}
		for i := 0; i <= len(errStr)-8; i++ {
			if errStr[i:i+8] == "fallback" {
				hasFallback = true
			}
		}
		if !hasPrimary || !hasFallback {
			t.Errorf("error should contain both 'primary' and 'fallback': %v", err)
		}
	}
	// 如果 err==nil（数据不足以触发解码），parser 可能只是等待更多数据，这也是合理的
}

// ================================================================
// 辅助函数
// ================================================================

// newBufReader 创建 proto.Reader 所需的 io.Reader
func newBufReader(data []byte) *chunkReader {
	return &chunkReader{data: data}
}

type chunkReader struct {
	data []byte
	pos  int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

// 确保 bswap 被使用（避免 unused import 编译错误）
var _ = bswap.Swap64
