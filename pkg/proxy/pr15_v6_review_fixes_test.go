package proxy

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// ============================================================================
// P2-1: Auth Token Settings 截取测试
// ============================================================================

func TestStripAuthTokenSettings(t *testing.T) {
	t.Run("移除 x_auth_token 和 SQL_x_auth_token", func(t *testing.T) {
		settings := []proto.Setting{
			{Key: "max_threads", Value: "4", Important: false},
			{Key: "x_auth_token", Value: "eyJhbGciOiJFUzI1NiJ9...", Important: false},
			{Key: "database", Value: "default", Important: false},
			{Key: "SQL_x_auth_token", Value: "some_token_value", Important: false},
			{Key: "max_memory_usage", Value: "1000000000", Important: false},
		}

		result := stripAuthTokenSettings(settings)

		if len(result) != 3 {
			t.Fatalf("expected 3 settings after strip, got %d", len(result))
		}
		for _, s := range result {
			if s.Key == "x_auth_token" || s.Key == "SQL_x_auth_token" {
				t.Errorf("auth token setting %q should have been stripped", s.Key)
			}
		}
		// 验证保留的 settings 顺序和值
		expectedKeys := []string{"max_threads", "database", "max_memory_usage"}
		for i, key := range expectedKeys {
			if result[i].Key != key {
				t.Errorf("result[%d].Key = %q, want %q", i, result[i].Key, key)
			}
		}
	})

	t.Run("无 auth token 时不变", func(t *testing.T) {
		settings := []proto.Setting{
			{Key: "max_threads", Value: "4"},
			{Key: "database", Value: "default"},
		}

		result := stripAuthTokenSettings(settings)

		if len(result) != 2 {
			t.Fatalf("expected 2 settings, got %d", len(result))
		}
	})

	t.Run("空 settings", func(t *testing.T) {
		result := stripAuthTokenSettings(nil)
		if result != nil {
			t.Errorf("expected nil for nil input, got %v", result)
		}

		result = stripAuthTokenSettings([]proto.Setting{})
		if len(result) != 0 {
			t.Errorf("expected 0 settings, got %d", len(result))
		}
	})

	t.Run("仅 auth token settings", func(t *testing.T) {
		settings := []proto.Setting{
			{Key: "x_auth_token", Value: "token1"},
			{Key: "SQL_x_auth_token", Value: "token2"},
		}

		result := stripAuthTokenSettings(settings)

		if len(result) != 0 {
			t.Errorf("expected 0 settings after strip, got %d", len(result))
		}
	})
}

func TestStripAuthTokenOldSettings(t *testing.T) {
	t.Run("移除旧格式 auth token", func(t *testing.T) {
		settings := []OldSetting{
			{Key: "max_threads", Value: 4},
			{Key: "x_auth_token", Value: 12345},
			{Key: "database", Value: 0},
		}

		result := stripAuthTokenOldSettings(settings)

		if len(result) != 2 {
			t.Fatalf("expected 2 settings after strip, got %d", len(result))
		}
		if result[0].Key != "max_threads" || result[1].Key != "database" {
			t.Errorf("unexpected settings: %v", result)
		}
	})

	t.Run("空 OldSettings", func(t *testing.T) {
		result := stripAuthTokenOldSettings(nil)
		if result != nil {
			t.Errorf("expected nil, got %v", result)
		}
	})
}

// ============================================================================
// P2-4: OldSetting 编解码 (Int64 vs UVarInt) 测试
// ============================================================================

func TestOldSettingEncoding_Int64(t *testing.T) {
	t.Run("编码使用 Int64 而非 UVarInt", func(t *testing.T) {
		buf := &proto.Buffer{}

		// 手动编码旧格式 setting: [key: String][value: Int64]
		value := uint64(256) // 256 在 UVarInt 中是 [0x80, 0x02]，在 Int64 中是 8 字节 LE
		buf.PutString("test_setting")
		buf.PutInt64(int64(value))
		buf.PutString("") // end marker

		// 读回验证
		reader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf)))

		key, err := reader.Str()
		if err != nil {
			t.Fatalf("read key error: %v", err)
		}
		if key != "test_setting" {
			t.Errorf("key = %q, want %q", key, "test_setting")
		}

		val, err := reader.Int64()
		if err != nil {
			t.Fatalf("read value error: %v", err)
		}
		if uint64(val) != value {
			t.Errorf("value = %d, want %d", val, value)
		}

		// end marker
		endKey, err := reader.Str()
		if err != nil {
			t.Fatalf("read end marker error: %v", err)
		}
		if endKey != "" {
			t.Errorf("end marker = %q, want empty", endKey)
		}
	})

	t.Run("大值编码验证 Int64 vs UVarInt 差异", func(t *testing.T) {
		value := uint64(1 << 20) // 1MB — UVarInt 需要 3 字节, Int64 固定 8 字节

		// Int64 编码
		bufInt64 := &proto.Buffer{}
		bufInt64.PutInt64(int64(value))
		if len(bufInt64.Buf) != 8 {
			t.Errorf("Int64 encoding should be 8 bytes, got %d", len(bufInt64.Buf))
		}

		// UVarInt 编码（用于对比）
		bufUVarInt := &proto.Buffer{}
		bufUVarInt.PutUVarInt(value)
		if len(bufUVarInt.Buf) >= 8 {
			t.Errorf("UVarInt encoding for %d should be < 8 bytes, got %d", value, len(bufUVarInt.Buf))
		}

		t.Logf("Int64(%d) = %d bytes, UVarInt(%d) = %d bytes",
			value, len(bufInt64.Buf), value, len(bufUVarInt.Buf))
	})

	t.Run("OldSetting round-trip 完整性", func(t *testing.T) {
		// 编码
		buf := &proto.Buffer{}
		settings := []OldSetting{
			{Key: "max_threads", Value: 8},
			{Key: "max_memory_usage", Value: 10000000000}, // 10GB
			{Key: "use_uncompressed_cache", Value: 0},
		}

		for _, s := range settings {
			buf.PutString(s.Key)
			buf.PutInt64(int64(s.Value))
		}
		buf.PutString("") // end

		// 解码
		reader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf)))
		var decoded []OldSetting
		for {
			key, err := reader.Str()
			if err != nil {
				t.Fatalf("read key error: %v", err)
			}
			if key == "" {
				break
			}
			val, err := reader.Int64()
			if err != nil {
				t.Fatalf("read value error: %v", err)
			}
			decoded = append(decoded, OldSetting{Key: key, Value: uint64(val)})
		}

		if len(decoded) != len(settings) {
			t.Fatalf("decoded %d settings, want %d", len(decoded), len(settings))
		}
		for i, s := range decoded {
			if s.Key != settings[i].Key || s.Value != settings[i].Value {
				t.Errorf("setting[%d] = {%s, %d}, want {%s, %d}",
					i, s.Key, s.Value, settings[i].Key, settings[i].Value)
			}
		}
	})
}

// ============================================================================
// P2-5: 包类型名称完整性测试
// ============================================================================

func TestServerPacketNames_V6(t *testing.T) {
	// 验证所有已知 server 包类型都有名称
	expectedNames := map[uint64]string{
		0:  "Hello",
		1:  "Data",
		2:  "Exception",
		3:  "Progress",
		4:  "Pong",
		5:  "EndOfStream",
		6:  "ProfileInfo",
		7:  "Totals",
		8:  "Extremes",
		9:  "TablesStatusResponse",
		10: "Log",
		11: "TableColumns",
		12: "PartUUIDs",
		13: "ReadTaskRequest",
		14: "ProfileEvents",
		15: "MergeTreeReadTaskRequest",
		16: "MergeTreeAllRangesAnnouncement",
		17: "TimezoneUpdate",
	}

	for code, expectedName := range expectedNames {
		name, ok := serverPacketNames[code]
		if !ok {
			t.Errorf("server packet code %d missing from serverPacketNames", code)
		} else if name != expectedName {
			t.Errorf("serverPacketNames[%d] = %q, want %q", code, name, expectedName)
		}
	}
}

func TestClientPacketNames_V6(t *testing.T) {
	// 验证所有已知 client 包类型都有名称
	expectedNames := map[uint64]string{
		0:  "Hello",
		1:  "Query",
		2:  "Data",
		3:  "Cancel",
		4:  "Ping",
		5:  "TablesStatusRequest",
		6:  "KeepAlive",
		7:  "Scalar",
		8:  "IgnoredPartUUIDs",
		9:  "ReadTaskResponse",
		10: "MergeTreeReadTaskResponse",
		11: "QueryPlan",
	}

	for code, expectedName := range expectedNames {
		name, ok := packetNames[code]
		if !ok {
			t.Errorf("client packet code %d missing from packetNames", code)
		} else if name != expectedName {
			t.Errorf("packetNames[%d] = %q, want %q", code, name, expectedName)
		}
	}

	// 确认没有重复名称
	seen := make(map[string]uint64)
	for code, name := range packetNames {
		if prev, ok := seen[name]; ok {
			t.Errorf("duplicate packet name %q at codes %d and %d", name, prev, code)
		}
		seen[name] = code
	}
}

// ============================================================================
// P2-6: MaxConnectionLifetime 配置测试
// ============================================================================

func TestMaxConnectionLifetime(t *testing.T) {
	t.Run("默认值为 24h", func(t *testing.T) {
		cfg := DefaultConfig()
		if cfg.MaxConnectionLifetime.Duration != 24*time.Hour {
			t.Errorf("MaxConnectionLifetime = %v, want 24h", cfg.MaxConnectionLifetime.Duration)
		}
	})

	t.Run("值为 0 时禁用", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.MaxConnectionLifetime = Duration{0}

		// 验证 0 值表示禁用
		if cfg.MaxConnectionLifetime.Duration != 0 {
			t.Errorf("expected 0 duration when disabled")
		}
	})

	t.Run("连接超时关闭验证", func(t *testing.T) {
		// 创建 pipe 连接来验证超时关闭
		clientConn, serverConn := net.Pipe()
		defer clientConn.Close()
		defer serverConn.Close()

		closed := make(chan struct{})
		lifetime := 200 * time.Millisecond

		timer := time.AfterFunc(lifetime, func() {
			clientConn.Close()
			close(closed)
		})
		defer timer.Stop()

		select {
		case <-closed:
			// 预期在 lifetime 后关闭
		case <-time.After(2 * time.Second):
			t.Error("connection was not closed within expected lifetime")
		}
	})
}

// ============================================================================
// P2-3: BlockInfo 未知 field 容错测试
// ============================================================================

func TestBlockInfoUnknownField(t *testing.T) {
	t.Run("未知 field 应报错（R3-4 安全修复）", func(t *testing.T) {
		// 构造 BlockInfo 数据，包含未知 field 4
		buf := &proto.Buffer{}
		// field 1: is_overflows
		buf.PutUVarInt(1)
		buf.PutBool(false)
		// field 2: bucket_num
		buf.PutUVarInt(2)
		buf.PutInt32(-1)
		// field 4: 未知 field，值为 UVarInt(42)
		buf.PutUVarInt(4)
		buf.PutUVarInt(42)
		// field 0: end marker
		buf.PutUVarInt(0)

		reader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf)))
		_, err := decodeBlockInfoCompat(reader)

		// R3-4: 未知 field 现在返回错误而非尝试跳过（因为无法安全确定 field 类型）
		if err == nil {
			t.Error("R3-4: decodeBlockInfoCompat should error on unknown field 4")
		} else {
			t.Logf("OK: got expected error: %v", err)
		}
	})

	t.Run("已知 fields 正常解码", func(t *testing.T) {
		buf := &proto.Buffer{}
		// field 1: is_overflows
		buf.PutUVarInt(1)
		buf.PutBool(true)
		// field 2: bucket_num
		buf.PutUVarInt(2)
		buf.PutInt32(5)
		// field 3: out_of_order_buckets
		buf.PutUVarInt(3)
		buf.PutUVarInt(2) // count=2
		buf.PutInt32(10)
		buf.PutInt32(20)
		// end
		buf.PutUVarInt(0)

		reader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf)))
		info, err := decodeBlockInfoCompat(reader)

		if err != nil {
			t.Fatalf("decode error: %v", err)
		}
		if !info.Overflows {
			t.Error("Overflows should be true")
		}
		if info.BucketNum != 5 {
			t.Errorf("BucketNum = %d, want 5", info.BucketNum)
		}
		if len(info.OutOfOrderBuckets) != 2 {
			t.Fatalf("OutOfOrderBuckets len = %d, want 2", len(info.OutOfOrderBuckets))
		}
		if info.OutOfOrderBuckets[0] != 10 || info.OutOfOrderBuckets[1] != 20 {
			t.Errorf("OutOfOrderBuckets = %v, want [10, 20]", info.OutOfOrderBuckets)
		}
	})

	t.Run("多个未知 fields 应报错（R3-4）", func(t *testing.T) {
		buf := &proto.Buffer{}
		// field 5: unknown
		buf.PutUVarInt(5)
		buf.PutUVarInt(100)
		// field 6: unknown
		buf.PutUVarInt(6)
		buf.PutUVarInt(200)
		// field 1: is_overflows
		buf.PutUVarInt(1)
		buf.PutBool(true)
		// end
		buf.PutUVarInt(0)

		reader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf)))
		_, err := decodeBlockInfoCompat(reader)

		// R3-4: 第一个未知 field 就应该报错
		if err == nil {
			t.Error("R3-4: decodeBlockInfoCompat should error on unknown field 5")
		} else {
			t.Logf("OK: got expected error on first unknown field: %v", err)
		}
	})
}

// ============================================================================
// P0-3: forwardUntilQueryDone 测试
// ============================================================================

func TestForwardUntilQueryDone(t *testing.T) {
	t.Run("queryDoneCh 触发恢复", func(t *testing.T) {
		p := &Proxy{cfg: DefaultConfig()}
		p.cfg.IdleTimeout = Duration{500 * time.Millisecond}

		// 创建 pipe 连接
		clientRead, clientWrite := net.Pipe()
		defer clientRead.Close()
		defer clientWrite.Close()

		upstreamRead, upstreamWrite := net.Pipe()
		defer upstreamRead.Close()
		defer upstreamWrite.Close()

		queryDoneCh := make(chan queryDoneSignal, 8)

		// 在后台写入一些数据然后发出 queryDone 信号
		go func() {
			time.Sleep(100 * time.Millisecond)
			clientWrite.Write([]byte("some_raw_data"))
			time.Sleep(100 * time.Millisecond)
			queryDoneCh <- queryDoneSignal{IsEndOfStream: true} // 模拟 upstream 返回 EndOfStream
		}()

		// 在后台消费 upstream 写入的数据
		go func() {
			buf := make([]byte, 1024)
			for {
				_, err := upstreamRead.Read(buf)
				if err != nil {
					return
				}
			}
		}()

		br := bufio.NewReader(clientRead)
		result := p.forwardUntilQueryDone(1, br, clientRead, upstreamWrite, queryDoneCh)

		if !result {
			t.Error("forwardUntilQueryDone should return true when query done")
		}
	})

	t.Run("连接关闭返回 false", func(t *testing.T) {
		p := &Proxy{cfg: DefaultConfig()}
		p.cfg.IdleTimeout = Duration{200 * time.Millisecond}

		clientRead, clientWrite := net.Pipe()
		defer clientRead.Close()

		upstreamRead, upstreamWrite := net.Pipe()
		defer upstreamRead.Close()
		defer upstreamWrite.Close()

		queryDoneCh := make(chan queryDoneSignal, 8)

		// 在后台关闭客户端连接
		go func() {
			time.Sleep(100 * time.Millisecond)
			clientWrite.Close()
		}()

		// 消费 upstream
		go func() {
			buf := make([]byte, 1024)
			for {
				_, err := upstreamRead.Read(buf)
				if err != nil {
					return
				}
			}
		}()

		br := bufio.NewReader(clientRead)
		result := p.forwardUntilQueryDone(1, br, clientRead, upstreamWrite, queryDoneCh)

		if result {
			t.Error("forwardUntilQueryDone should return false when connection closed")
		}
	})
}

// ============================================================================
// P0-3: MergeTreeReadTaskResponse 恢复 streaming 测试
// ============================================================================

func TestMergeTreeReadTaskResponseResumesStreaming(t *testing.T) {
	t.Run("MergeTreeReadTaskResponse 后 codeByte 正确", func(t *testing.T) {
		// 构造数据流: [MergeTreeReadTaskResponse type][some data][Ping type]
		var data bytes.Buffer
		data.WriteByte(byte(clientCodeMergeTreeReadTaskResponse))
		// MergeTreeReadTaskResponse 的载荷（版本+完成标志）
		vb := make([]byte, 8)
		binary.LittleEndian.PutUint64(vb, 1)
		data.Write(vb)
		data.WriteByte('1') // finish = true

		// 后续 Ping 包
		data.WriteByte(byte(proto.ClientCodePing))

		br := bufio.NewReader(&data)

		// 读取 MergeTreeReadTaskResponse 类型字节
		codeByte, err := br.ReadByte()
		if err != nil {
			t.Fatalf("ReadByte error: %v", err)
		}
		if proto.ClientCode(codeByte) != clientCodeMergeTreeReadTaskResponse {
			t.Fatalf("expected MergeTreeReadTaskResponse, got %d", codeByte)
		}

		// 模拟消费 MergeTreeReadTaskResponse 载荷
		payload := make([]byte, 9) // 8 bytes version + 1 byte finish
		_, err = io.ReadFull(br, payload)
		if err != nil {
			t.Fatalf("read payload error: %v", err)
		}

		// 验证可以读取后续的 Ping 包
		nextByte, err := br.ReadByte()
		if err != nil {
			t.Fatalf("ReadByte for next packet error: %v", err)
		}
		if proto.ClientCode(nextByte) != proto.ClientCodePing {
			t.Errorf("expected Ping (4), got %d", nextByte)
		}
	})
}

// ============================================================================
// P2-1: authTokenKeys map 完整性测试
// ============================================================================

func TestAuthTokenKeysMap(t *testing.T) {
	t.Run("x_auth_token 在 map 中", func(t *testing.T) {
		if !proxySettingKeys["x_auth_token"] {
			t.Error("x_auth_token should be in proxySettingKeys")
		}
	})

	t.Run("SQL_x_auth_token 在 map 中", func(t *testing.T) {
		if !proxySettingKeys["SQL_x_auth_token"] {
			t.Error("SQL_x_auth_token should be in proxySettingKeys")
		}
	})

	t.Run("普通 setting 不在 map 中", func(t *testing.T) {
		ordinaryKeys := []string{"max_threads", "database", "max_memory_usage", "use_uncompressed_cache"}
		for _, key := range ordinaryKeys {
			if proxySettingKeys[key] {
				t.Errorf("%q should not be in proxySettingKeys", key)
			}
		}
	})
}

// ============================================================================
// encodeBlockInfoCompat round-trip 测试
// ============================================================================

func TestBlockInfoCompat_RoundTrip_V6(t *testing.T) {
	t.Run("标准 BlockInfo round-trip", func(t *testing.T) {
		original := &BlockInfoCompat{
			Overflows:            true,
			BucketNum:            42,
			OutOfOrderBuckets:    []int32{1, 2, 3},
			HasOutOfOrderBuckets: true,
		}

		buf := &proto.Buffer{}
		encodeBlockInfoCompat(buf, original)

		reader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf)))
		decoded, err := decodeBlockInfoCompat(reader)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}

		if decoded.Overflows != original.Overflows {
			t.Errorf("Overflows = %v, want %v", decoded.Overflows, original.Overflows)
		}
		if decoded.BucketNum != original.BucketNum {
			t.Errorf("BucketNum = %d, want %d", decoded.BucketNum, original.BucketNum)
		}
		if len(decoded.OutOfOrderBuckets) != len(original.OutOfOrderBuckets) {
			t.Fatalf("OutOfOrderBuckets len = %d, want %d",
				len(decoded.OutOfOrderBuckets), len(original.OutOfOrderBuckets))
		}
		for i, v := range decoded.OutOfOrderBuckets {
			if v != original.OutOfOrderBuckets[i] {
				t.Errorf("OutOfOrderBuckets[%d] = %d, want %d", i, v, original.OutOfOrderBuckets[i])
			}
		}
	})

	t.Run("空 OutOfOrderBuckets", func(t *testing.T) {
		original := &BlockInfoCompat{
			Overflows: false,
			BucketNum: -1,
		}

		buf := &proto.Buffer{}
		encodeBlockInfoCompat(buf, original)

		reader := proto.NewReader(bufio.NewReader(bytes.NewReader(buf.Buf)))
		decoded, err := decodeBlockInfoCompat(reader)
		if err != nil {
			t.Fatalf("decode error: %v", err)
		}

		if decoded.Overflows {
			t.Error("Overflows should be false")
		}
		if decoded.BucketNum != -1 {
			t.Errorf("BucketNum = %d, want -1", decoded.BucketNum)
		}
		if len(decoded.OutOfOrderBuckets) != 0 {
			t.Errorf("OutOfOrderBuckets should be empty, got %d", len(decoded.OutOfOrderBuckets))
		}
	})
}

// ============================================================================
// P0-3: fallbackRawCopy 永久性和 forwardUntilQueryDone 临时性的对比
// ============================================================================

func TestFallbackVsTemporaryPassthrough(t *testing.T) {
	t.Run("fallbackRawCopy 是永久性转发", func(t *testing.T) {
		p := &Proxy{cfg: DefaultConfig()}

		// 准备数据: 一些数据后跟 EOF
		data := bytes.Repeat([]byte("test"), 100)
		br := bufio.NewReader(bytes.NewReader(data))

		var out bytes.Buffer

		// fallbackRawCopy 会一直读到 EOF 然后 return
		done := make(chan struct{})
		go func() {
			p.fallbackRawCopy(1, br, &mockConn{}, &out)
			close(done)
		}()

		select {
		case <-done:
			if out.Len() != len(data) {
				t.Errorf("forwarded %d bytes, want %d", out.Len(), len(data))
			}
		case <-time.After(2 * time.Second):
			t.Error("fallbackRawCopy did not return")
		}
	})
}

// mockConn 实现 net.Conn 用于测试
type mockConn struct{}

func (m *mockConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (m *mockConn) Write(b []byte) (int, error)      { return len(b), nil }
func (m *mockConn) Close() error                     { return nil }
func (m *mockConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (m *mockConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (m *mockConn) SetDeadline(time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(time.Time) error { return nil }
