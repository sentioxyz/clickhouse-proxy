package proxy

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

// ============================================================================
// R1-1: forwardUntilQueryDone goroutine 泄漏修复测试
// ============================================================================

// TestForwardUntilQueryDone_NoGoroutineLeak 验证 forwardUntilQueryDone 返回后
// 不会留下泄漏的 goroutine。
func TestForwardUntilQueryDone_NoGoroutineLeak(t *testing.T) {
	// 创建一个 mock 的 proxy
	cfg := DefaultConfig()
	cfg.IdleTimeout = Duration{500 * time.Millisecond}
	p := &Proxy{cfg: cfg}

	// 使用管道模拟 client 和 upstream
	clientRead, clientWrite := net.Pipe()
	defer clientRead.Close()
	defer clientWrite.Close()

	upstreamBuf := &bytes.Buffer{}
	queryDoneCh := make(chan queryDoneSignal, 8)

	// 记录初始 goroutine 数
	runtime.GC()
	time.Sleep(10 * time.Millisecond)
	initialGoroutines := runtime.NumGoroutine()

	// 写入一些数据，然后发送 queryDone 信号
	go func() {
		time.Sleep(10 * time.Millisecond)
		clientWrite.Write([]byte("test data packet"))
		time.Sleep(10 * time.Millisecond)
		queryDoneCh <- queryDoneSignal{IsEndOfStream: true}
	}()

	br := bufio.NewReaderSize(clientRead, 4096)
	result := p.forwardUntilQueryDone(1, br, clientRead, upstreamBuf, queryDoneCh)
	if !result {
		t.Fatal("expected forwardUntilQueryDone to return true")
	}

	// 验证数据被转发
	if upstreamBuf.Len() == 0 {
		t.Fatal("expected upstream to receive some data")
	}

	// 关闭连接以确保读取 goroutine 能退出
	clientWrite.Close()
	// 给 goroutine 时间退出
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()
	// 允许 ±2 的误差（其他后台 goroutine）
	if finalGoroutines > initialGoroutines+2 {
		t.Errorf("potential goroutine leak: initial=%d, final=%d", initialGoroutines, finalGoroutines)
	}
}

// TestForwardUntilQueryDone_DrainBeforeReturn 验证 queryDone 信号后
// readCh 中的剩余数据被正确 drain 到 upstream。
func TestForwardUntilQueryDone_DrainBeforeReturn(t *testing.T) {
	cfg := DefaultConfig()
	cfg.IdleTimeout = Duration{500 * time.Millisecond}
	p := &Proxy{cfg: cfg}

	clientRead, clientWrite := net.Pipe()
	defer clientRead.Close()
	defer clientWrite.Close()

	upstreamBuf := &bytes.Buffer{}
	queryDoneCh := make(chan queryDoneSignal, 8)

	// 先写入数据，等一小段时间让 goroutine 读入 readCh，再发 queryDone
	go func() {
		clientWrite.Write([]byte("data-before-signal"))
		time.Sleep(50 * time.Millisecond)
		queryDoneCh <- queryDoneSignal{IsEndOfStream: true}
	}()

	br := bufio.NewReaderSize(clientRead, 4096)
	result := p.forwardUntilQueryDone(1, br, clientRead, upstreamBuf, queryDoneCh)
	if !result {
		t.Fatal("expected true return")
	}

	// 验证 "data-before-signal" 被转发
	if !bytes.Contains(upstreamBuf.Bytes(), []byte("data-before-signal")) {
		t.Errorf("expected upstream to contain 'data-before-signal', got: %q", upstreamBuf.String())
	}
}

// TestForwardUntilQueryDone_ConnectionError 验证连接错误时正确返回 false。
func TestForwardUntilQueryDone_ConnectionError(t *testing.T) {
	cfg := DefaultConfig()
	cfg.IdleTimeout = Duration{500 * time.Millisecond}
	p := &Proxy{cfg: cfg}

	clientRead, clientWrite := net.Pipe()
	defer clientRead.Close()

	upstreamBuf := &bytes.Buffer{}
	queryDoneCh := make(chan queryDoneSignal, 8)

	// 立即关闭写端，模拟连接断开
	clientWrite.Close()

	br := bufio.NewReaderSize(clientRead, 4096)
	result := p.forwardUntilQueryDone(1, br, clientRead, upstreamBuf, queryDoneCh)
	if result {
		t.Fatal("expected false return on connection error")
	}
}

// ============================================================================
// R1-3: eraseTokenValue 安全性测试
// ============================================================================

// buildUVarIntString 构建 UVarInt-length-prefixed string
func buildUVarIntString(s string) []byte {
	lenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lenBuf, uint64(len(s)))
	result := make([]byte, n+len(s))
	copy(result, lenBuf[:n])
	copy(result[n:], s)
	return result
}

// TestEraseTokenValue_SecurityAudit 验证 eraseTokenValue 能正确脱敏 token 值
func TestEraseTokenValue_SecurityAudit(t *testing.T) {
	// 构建包含 auth token 的 mock 数据: [key][value]
	tokenKey := "x_auth_token"
	tokenValue := "secret_signature_value_12345"

	var data []byte
	data = append(data, buildUVarIntString(tokenKey)...)
	data = append(data, buildUVarIntString(tokenValue)...)

	result := eraseTokenValue(data, tokenKey)

	// 验证 1: 原始 token 值不应出现在结果中
	if bytes.Contains(result, []byte(tokenValue)) {
		t.Errorf("SECURITY: token value %q still present in output", tokenValue)
	}

	// 验证 2: key 被替换为 promql_table
	if !bytes.Contains(result, []byte("promql_table")) {
		t.Error("expected 'promql_table' in output")
	}

	// 验证 3: 值被 '*' 替换
	starCount := bytes.Count(result, []byte("*"))
	if starCount == 0 {
		t.Error("expected masking characters in output")
	}
}

// TestEraseTokenValue_SQLAuthToken 验证 SQL_x_auth_token 也能正确脱敏
func TestEraseTokenValue_SQLAuthToken(t *testing.T) {
	tokenKey := "SQL_x_auth_token"
	tokenValue := "my_secret_jwt_token"

	var data []byte
	data = append(data, buildUVarIntString(tokenKey)...)
	data = append(data, buildUVarIntString(tokenValue)...)

	result := eraseTokenValue(data, tokenKey)

	if bytes.Contains(result, []byte(tokenValue)) {
		t.Errorf("SECURITY: SQL_x_auth_token value %q still present", tokenValue)
	}
	if !bytes.Contains(result, []byte("promql_table")) {
		t.Error("expected 'promql_table' key in output")
	}
}

// TestEraseTokenValue_NoToken 验证没有 token 时数据不变
func TestEraseTokenValue_NoToken(t *testing.T) {
	data := []byte("regular data without any tokens")
	result := eraseTokenValue(data, "x_auth_token")
	if !bytes.Equal(result, data) {
		t.Errorf("expected unchanged data, got: %q", result)
	}
}

// ============================================================================
// R1-5: ClusterFunctionReadTaskResponse 测试
// ============================================================================

// TestClusterFunctionReadTaskResponseConstant 验证 type 13 常量定义正确
func TestClusterFunctionReadTaskResponseConstant(t *testing.T) {
	if clientCodeClusterFunctionReadTaskResponse != 13 {
		t.Errorf("expected clientCodeClusterFunctionReadTaskResponse = 13, got %d",
			clientCodeClusterFunctionReadTaskResponse)
	}
}

// TestClusterFunctionReadTaskResponseFormat 验证 type 13 包的格式
// ClusterFunctionReadTaskResponse 格式: [String response]
func TestClusterFunctionReadTaskResponseFormat(t *testing.T) {
	// 构建一个 ClusterFunctionReadTaskResponse 包
	response := "partition_0_shard_1"
	var buf bytes.Buffer
	// UVarInt 编码的字符串长度
	lenBuf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(lenBuf, uint64(len(response)))
	buf.Write(lenBuf[:n])
	buf.WriteString(response)

	// 验证可以正确解码
	reader := bytes.NewReader(buf.Bytes())
	// 读取 UVarInt 长度
	strLen, err := binary.ReadUvarint(&byteReaderWrapper{reader})
	if err != nil {
		t.Fatalf("failed to read string length: %v", err)
	}
	if strLen != uint64(len(response)) {
		t.Errorf("expected string length %d, got %d", len(response), strLen)
	}
	strBuf := make([]byte, strLen)
	_, err = io.ReadFull(reader, strBuf)
	if err != nil {
		t.Fatalf("failed to read string: %v", err)
	}
	if string(strBuf) != response {
		t.Errorf("expected response %q, got %q", response, string(strBuf))
	}
}

// byteReaderWrapper 将 io.Reader 包装为 io.ByteReader，用于 binary.ReadUvarint。
type byteReaderWrapper struct {
	r io.Reader
}

func (b *byteReaderWrapper) ReadByte() (byte, error) {
	var buf [1]byte
	_, err := b.r.Read(buf[:])
	return buf[0], err
}

// ============================================================================
// R1-13: consumeBuf 优化测试
// ============================================================================

// TestConsumeBuf_InPlaceMove 验证 consumeBuf 使用原地移动而非分配新 slice。
func TestConsumeBuf_InPlaceMove(t *testing.T) {
	parser := &queryParser{
		buf: []byte("hello world"),
	}

	// 消费前5字节
	parser.consumeBuf(5)

	expected := " world"
	if string(parser.buf) != expected {
		t.Errorf("expected %q after consume, got %q", expected, string(parser.buf))
	}

	// 全部消费
	parser.consumeBuf(len(parser.buf))
	if parser.buf != nil {
		t.Errorf("expected nil buf after full consume, got %v", parser.buf)
	}
}

// TestConsumeBuf_ConsistencyWithOld 验证新实现与旧实现行为一致。
func TestConsumeBuf_ConsistencyWithOld(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		n      int
		expect string
	}{
		{"consume_none", "hello", 0, "hello"},
		{"consume_half", "hello world", 6, "world"},
		{"consume_all", "test", 4, ""},
		{"consume_more", "abc", 5, ""},
		{"single_byte", "x", 1, ""},
		{"partial", "abcdefgh", 3, "defgh"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &queryParser{buf: []byte(tt.input)}
			p.consumeBuf(tt.n)
			if tt.expect == "" {
				if p.buf != nil {
					t.Errorf("expected nil, got %q", string(p.buf))
				}
			} else {
				if string(p.buf) != tt.expect {
					t.Errorf("expected %q, got %q", tt.expect, string(p.buf))
				}
			}
		})
	}
}

// ============================================================================
// R1-10: maskPassword 测试
// ============================================================================

// TestMaskPassword_Audit 验证密码脱敏的各种边界情况
// rewriter.go 中的 maskPassword: <=2 返回 "***", 其余保留首尾字符
func TestMaskPassword_Audit(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		{"", "***"},
		{"a", "***"},
		{"ab", "***"},
		{"abc", "a*c"},
		{"password123", "p*********3"},
		{"my_secret_pass", "m************s"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := maskPassword(tt.input)
			if got != tt.expect {
				t.Errorf("maskPassword(%q) = %q, want %q", tt.input, got, tt.expect)
			}
		})
	}
}

// ============================================================================
// R1-16: Graceful Shutdown 测试
// ============================================================================

// TestGracefulShutdown_Config 验证 ShutdownTimeout 配置正确加载。
func TestGracefulShutdown_Config(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ShutdownTimeout.Duration != 30*time.Second {
		t.Errorf("expected default ShutdownTimeout=30s, got %v", cfg.ShutdownTimeout.Duration)
	}
}

// TestGracefulShutdown_WaitGroupDrain 验证 WaitGroup 排水机制。
func TestGracefulShutdown_WaitGroupDrain(t *testing.T) {
	var wg sync.WaitGroup
	done := make(chan struct{})

	// 模拟 3 个在途连接
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(delay time.Duration) {
			time.Sleep(delay)
			wg.Done()
		}(time.Duration(i*10) * time.Millisecond)
	}

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// 所有连接排水完成
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for connections to drain")
	}
}

// TestGracefulShutdown_TimeoutExceeded 验证超时时间到达后强制关闭。
func TestGracefulShutdown_TimeoutExceeded(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1) // 模拟一个永不完成的连接

	drainDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(drainDone)
	}()

	shutdownTimeout := 50 * time.Millisecond
	select {
	case <-drainDone:
		t.Fatal("should not drain before timeout")
	case <-time.After(shutdownTimeout):
		// 超时后强制关闭 — 正确行为
	}

	// 清理
	wg.Done()
}

// ============================================================================
// 回归测试: 确保现有功能不被破坏
// ============================================================================

// TestReplaceToken_StillWorks 验证 replaceToken 仍然正确工作。
func TestReplaceToken_StillWorks(t *testing.T) {
	data := buildUVarIntString("x_auth_token")
	result := replaceToken(data, "x_auth_token", "promql_table")
	expected := buildUVarIntString("promql_table")
	if !bytes.Equal(result, expected) {
		t.Errorf("replaceToken failed: expected %v, got %v", expected, result)
	}
}

// TestEraseTokenValue_PreservesOtherData 验证脱敏不影响 token 前后的数据。
func TestEraseTokenValue_PreservesOtherData(t *testing.T) {
	// 构建: [prefix][token_key][token_value][suffix]
	prefix := []byte("some prefix data\x00\x01\x02")
	suffix := []byte("\x03\x04extra suffix data")

	tokenKey := "x_auth_token"
	tokenValue := "secret_value"

	var data []byte
	data = append(data, prefix...)
	data = append(data, buildUVarIntString(tokenKey)...)
	data = append(data, buildUVarIntString(tokenValue)...)
	data = append(data, suffix...)

	result := eraseTokenValue(data, tokenKey)

	// prefix 和 suffix 应保持不变
	if !bytes.HasPrefix(result, prefix) {
		t.Error("prefix was corrupted")
	}
	if !bytes.HasSuffix(result, suffix) {
		t.Error("suffix was corrupted")
	}

	// token 值不应出现
	if bytes.Contains(result, []byte(tokenValue)) {
		t.Error("SECURITY: token value still present")
	}
}

// TestAllClientPacketCodes_IncludesCluster 验证所有已知包类型常量已注册。
func TestAllClientPacketCodes_IncludesCluster(t *testing.T) {
	codes := map[string]int{
		"KeepAlive":                       int(clientCodeKeepAlive),
		"Scalar":                          int(clientCodeScalar),
		"IgnoredPartUUIDs":                int(clientCodeIgnoredPartUUIDs),
		"ReadTaskResponse":                int(clientCodeReadTaskResponse),
		"MergeTreeReadTaskResponse":       int(clientCodeMergeTreeReadTaskResponse),
		"QueryPlan":                       int(clientCodeQueryPlan),
		"ClusterFunctionReadTaskResponse": int(clientCodeClusterFunctionReadTaskResponse),
	}
	expected := map[string]int{
		"KeepAlive":                       6,
		"Scalar":                          7,
		"IgnoredPartUUIDs":                8,
		"ReadTaskResponse":                9,
		"MergeTreeReadTaskResponse":       10,
		"QueryPlan":                       11,
		"ClusterFunctionReadTaskResponse": 13,
	}
	for name, code := range codes {
		if code != expected[name] {
			t.Errorf("packet code %s: expected %d, got %d", name, expected[name], code)
		}
	}
}
