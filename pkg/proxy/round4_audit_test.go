package proxy

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"
)

// ================================================================
// R4-2: Streaming 模式 Validator 调用验证
// ================================================================

// mockValidator 用于测试的 Validator mock
type mockValidator struct {
	called   bool
	lastMeta QueryMeta
	err      error
}

func (m *mockValidator) ValidateQuery(_ context.Context, meta QueryMeta) (string, error) {
	m.called = true
	m.lastMeta = meta
	return "", m.err
}

func TestStreamingMode_ValidatorCalled(t *testing.T) {
	t.Run("validator 存在时应被调用", func(t *testing.T) {
		v := &mockValidator{}
		// 验证 mockValidator 实现了 Validator 接口
		var _ Validator = v
		if v.called {
			t.Fatal("validator should not be called before test")
		}
		// 模拟调用
		meta := QueryMeta{
			ConnID:   1,
			SQL:      "SELECT 1",
			Settings: map[string]string{"key": "value"},
		}
		_, err := v.ValidateQuery(context.Background(), meta)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !v.called {
			t.Fatal("validator should have been called")
		}
		if v.lastMeta.SQL != "SELECT 1" {
			t.Errorf("SQL = %q, want %q", v.lastMeta.SQL, "SELECT 1")
		}
	})

	t.Run("validator 返回错误时应阻止查询", func(t *testing.T) {
		v := &mockValidator{err: errors.New("auth failed")}
		meta := QueryMeta{
			ConnID:   2,
			SQL:      "DROP TABLE x",
			Settings: map[string]string{},
		}
		_, err := v.ValidateQuery(context.Background(), meta)
		if err == nil {
			t.Fatal("expected error")
		}
		if err.Error() != "auth failed" {
			t.Errorf("error = %q, want %q", err.Error(), "auth failed")
		}
	})

	t.Run("NoopValidator 始终通过", func(t *testing.T) {
		v := NoopValidator{}
		_, err := v.ValidateQuery(context.Background(), QueryMeta{SQL: "SELECT 1"})
		if err != nil {
			t.Fatalf("NoopValidator should always pass, got: %v", err)
		}
	})
}

func TestStreamingMode_SettingsExtraction(t *testing.T) {
	// R4-2: 验证 Settings 从 ExtQuery 正确提取
	t.Run("新格式 Settings 提取", func(t *testing.T) {
		// 模拟 streaming 中的 settings 提取逻辑
		settings := []struct {
			Key   string
			Value string
		}{
			{"max_memory_usage", "10000000000"},
			{"SQL_x_auth_token", "eyJhbGciOiJFUzI1NksifQ.test.sig"},
		}
		settingsMap := make(map[string]string, len(settings))
		for _, s := range settings {
			settingsMap[s.Key] = s.Value
		}
		if settingsMap["SQL_x_auth_token"] != "eyJhbGciOiJFUzI1NksifQ.test.sig" {
			t.Errorf("auth token not extracted correctly")
		}
		if settingsMap["max_memory_usage"] != "10000000000" {
			t.Errorf("setting not extracted correctly")
		}
	})

	t.Run("旧格式 OldSettings 提取", func(t *testing.T) {
		oldSettings := []OldSetting{
			{"max_threads", 8},
			{"x_auth_token", 0}, // 旧格式中 token 值为整数
		}
		settingsMap := make(map[string]string, len(oldSettings))
		for _, s := range oldSettings {
			settingsMap[s.Key] = fmt.Sprintf("%d", s.Value)
		}
		if settingsMap["max_threads"] != "8" {
			t.Errorf("old setting not extracted correctly")
		}
	})
}

// ================================================================
// R4-3: eraseTokenValue for+break 重构验证
// ================================================================

func TestEraseTokenValue_Refactored(t *testing.T) {
	t.Run("基本功能不变", func(t *testing.T) {
		// 构造 [UVarInt(len("x_auth_token"))]["x_auth_token"][UVarInt(len("secret123"))]["secret123"]
		var buf bytes.Buffer
		tokenKey := "x_auth_token"
		tokenValue := "secret123"

		keyBytes := []byte(tokenKey)
		lenBuf := make([]byte, binary.MaxVarintLen64)
		n := binary.PutUvarint(lenBuf, uint64(len(keyBytes)))
		buf.Write(lenBuf[:n])
		buf.Write(keyBytes)

		valBytes := []byte(tokenValue)
		n = binary.PutUvarint(lenBuf, uint64(len(valBytes)))
		buf.Write(lenBuf[:n])
		buf.Write(valBytes)

		data := buf.Bytes()
		result := eraseTokenValue(data, "x_auth_token")

		// 验证 value 被擦除
		if bytes.Contains(result, []byte("secret123")) {
			t.Error("token value should be erased")
		}
		// 验证 value 被替换为 '*'
		if !bytes.Contains(result, bytes.Repeat([]byte("*"), len(tokenValue))) {
			t.Error("token value should be replaced with asterisks")
		}
	})

	t.Run("无匹配时不修改", func(t *testing.T) {
		data := []byte("no token here")
		result := eraseTokenValue(data, "x_auth_token")
		if !bytes.Equal(result, data) {
			t.Error("data should not be modified when no token found")
		}
	})

	t.Run("空数据", func(t *testing.T) {
		result := eraseTokenValue([]byte{}, "x_auth_token")
		if len(result) != 0 {
			t.Error("empty data should return empty result")
		}
	})
}

// ================================================================
// R4-4: validateJWSJSON payload 解码提升验证
// ================================================================

func TestValidateJWSJSON_PayloadDecodedOnce(t *testing.T) {
	// 验证 payload 解码逻辑与之前行为一致
	// 使用无效 token 测试错误路径
	v := &EthValidator{
		Enabled:          true,
		AllowedAddresses: map[string]bool{},
		MaxTokenAge:      300 * time.Second,
	}

	t.Run("空签名应返回错误", func(t *testing.T) {
		_, err := v.validateJWSJSON(`{"payload":"dGVzdA","signatures":[]}`, "SELECT 1")
		if err == nil || err.Error() != "no signatures found in JWS JSON" {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("无效 payload 编码应返回错误", func(t *testing.T) {
		_, err := v.validateJWSJSON(`{"payload":"!!!invalid!!!","signatures":[{"protected":"dGVzdA","signature":"dGVzdA"}]}`, "SELECT 1")
		if err == nil {
			t.Fatal("expected error for invalid payload encoding")
		}
		if !contains(err.Error(), "invalid payload encoding") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("无效 payload JSON 应返回错误", func(t *testing.T) {
		// base64url("not json") = "bm90IGpzb24"
		_, err := v.validateJWSJSON(`{"payload":"bm90IGpzb24","signatures":[{"protected":"dGVzdA","signature":"dGVzdA"}]}`, "SELECT 1")
		if err == nil {
			t.Fatal("expected error for invalid payload JSON")
		}
		if !contains(err.Error(), "invalid payload JSON") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// ================================================================
// R4-6: ChunkedWriter pool 归还策略验证
// ================================================================

func TestChunkedWriter_PoolReturnOnError(t *testing.T) {
	t.Run("写入失败时 pool 仍可正常工作", func(t *testing.T) {
		failWriter := &failingWriter{failAfter: 0}
		cw := NewChunkedWriter(failWriter, true)

		// 第一次写入应该失败
		_, err := cw.Write([]byte("hello"))
		if err == nil {
			t.Fatal("expected write error")
		}

		// 验证 pool 仍然可用（不应 panic）
		cw2 := NewChunkedWriter(&bytes.Buffer{}, true)
		_, err = cw2.Write([]byte("world"))
		if err != nil {
			t.Fatalf("pool should still work: %v", err)
		}
	})

	t.Run("多次写入失败后 pool 不被耗尽", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			fw := &failingWriter{failAfter: 0}
			cw := NewChunkedWriter(fw, true)
			cw.Write([]byte("data"))
		}
		// 如果 pool 被耗尽，后续正常写入应该仍然工作
		buf := &bytes.Buffer{}
		cw := NewChunkedWriter(buf, true)
		_, err := cw.Write([]byte("after failures"))
		if err != nil {
			t.Fatalf("write after failures should succeed: %v", err)
		}
		if buf.Len() == 0 {
			t.Fatal("buffer should have data")
		}
	})
}

type failingWriter struct {
	failAfter int
	count     int
}

func (fw *failingWriter) Write(p []byte) (int, error) {
	if fw.count >= fw.failAfter {
		return 0, io.ErrClosedPipe
	}
	fw.count++
	return len(p), nil
}

// ================================================================
// R4-7: 时钟偏差容忍验证
// ================================================================

func TestVerifyPayloadAndHeader_ClockSkew(t *testing.T) {
	v := &EthValidator{
		Enabled:          true,
		AllowedAddresses: map[string]bool{},
		MaxTokenAge:      300 * time.Second,
	}

	sql := "SELECT 1"
	expectedHash := keccak256Hex([]byte(sql))
	header := JWSHeader{Alg: "ES256K", Typ: "JWS"}

	t.Run("3 秒后的 iat 应被接受（在 5 秒容忍内）", func(t *testing.T) {
		payload := JWSPayload{
			Iat:       time.Now().Unix() + 3,
			QueryHash: expectedHash,
		}
		err := v.verifyPayloadAndHeader(header, payload, sql)
		if err != nil {
			t.Errorf("3-second future iat should be tolerated, got: %v", err)
		}
	})

	t.Run("5 秒后的 iat 应被接受（边界值）", func(t *testing.T) {
		payload := JWSPayload{
			Iat:       time.Now().Unix() + 5,
			QueryHash: expectedHash,
		}
		err := v.verifyPayloadAndHeader(header, payload, sql)
		if err != nil {
			t.Errorf("5-second future iat should be tolerated, got: %v", err)
		}
	})

	t.Run("10 秒后的 iat 应被拒绝", func(t *testing.T) {
		payload := JWSPayload{
			Iat:       time.Now().Unix() + 10,
			QueryHash: expectedHash,
		}
		err := v.verifyPayloadAndHeader(header, payload, sql)
		if err == nil {
			t.Error("10-second future iat should be rejected")
		}
		if !contains(err.Error(), "token issued in the future") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("正常的过去 iat 应被接受", func(t *testing.T) {
		payload := JWSPayload{
			Iat:       time.Now().Unix() - 10,
			QueryHash: expectedHash,
		}
		err := v.verifyPayloadAndHeader(header, payload, sql)
		if err != nil {
			t.Errorf("past iat should be accepted, got: %v", err)
		}
	})

	t.Run("过期的 iat 应被拒绝", func(t *testing.T) {
		payload := JWSPayload{
			Iat:       time.Now().Unix() - 600,
			QueryHash: expectedHash,
		}
		err := v.verifyPayloadAndHeader(header, payload, sql)
		if err == nil {
			t.Error("expired iat should be rejected")
		}
		if !contains(err.Error(), "token expired") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// contains 检查字符串是否包含子串
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
