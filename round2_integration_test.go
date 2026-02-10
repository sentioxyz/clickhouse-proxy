//go:build integration

// 集成测试 - 需要本地 ClickHouse 实例在 localhost:9000 运行
// 运行方式: go test -v -count=1 -tags integration -run TestIntegration ./...

package main

import (
	"bufio"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

const (
	integrationCHAddr  = "localhost:9000"
	integrationTimeout = 5 * time.Second
)

func skipIfNoCH(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", integrationCHAddr, 2*time.Second)
	if err != nil {
		t.Skipf("ClickHouse not available at %s: %v", integrationCHAddr, err)
	}
	conn.Close()
}

// TestIntegration_DirectConnect 直连 ClickHouse 验证基本协议兼容性
func TestIntegration_DirectConnect(t *testing.T) {
	skipIfNoCH(t)

	conn, err := net.DialTimeout("tcp", integrationCHAddr, integrationTimeout)
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	br := bufio.NewReader(conn)
	chReader := proto.NewReader(br)

	// 发送 ClientHello
	buf := &proto.Buffer{}
	proto.ClientCodeHello.Encode(buf)
	buf.PutString("integration_test")     // client_name
	buf.PutUVarInt(24)                    // version_major
	buf.PutUVarInt(1)                     // version_minor
	buf.PutUVarInt(uint64(proto.Version)) // client_revision
	buf.PutString("default")              // database
	buf.PutString("default")              // user
	buf.PutString("")                     // password

	if _, err := conn.Write(buf.Buf); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	// 读取 ServerHello
	code, err := chReader.UVarInt()
	if err != nil {
		t.Fatalf("read server response code: %v", err)
	}

	if proto.ServerCode(code) == proto.ServerCodeHello {
		serverName, _ := chReader.Str()
		major, _ := chReader.UVarInt()
		minor, _ := chReader.UVarInt()
		revision, _ := chReader.UVarInt()
		t.Logf("Connected to ClickHouse: %s %d.%d (revision %d)", serverName, major, minor, revision)
	} else if proto.ServerCode(code) == proto.ServerCodeException {
		t.Logf("Server returned exception (code=%d), this may be expected for auth issues", code)
	} else {
		t.Fatalf("unexpected server response code: %d", code)
	}
}

// TestIntegration_ProxyPassthrough 通过 proxy 连接 ClickHouse
// 需要先启动 proxy: go run . --config config.yaml
func TestIntegration_ProxyPassthrough(t *testing.T) {
	proxyAddr := "localhost:9001" // 默认 proxy 监听端口
	conn, err := net.DialTimeout("tcp", proxyAddr, 2*time.Second)
	if err != nil {
		t.Skipf("Proxy not available at %s: %v", proxyAddr, err)
	}
	defer conn.Close()

	br := bufio.NewReader(conn)
	chReader := proto.NewReader(br)

	// 发送 ClientHello
	buf := &proto.Buffer{}
	proto.ClientCodeHello.Encode(buf)
	buf.PutString("proxy_integration_test")
	buf.PutUVarInt(24)
	buf.PutUVarInt(1)
	buf.PutUVarInt(uint64(proto.Version))
	buf.PutString("default")
	buf.PutString("default")
	buf.PutString("")

	if _, err := conn.Write(buf.Buf); err != nil {
		t.Fatalf("write hello: %v", err)
	}

	// 读取 ServerHello（经过 proxy 透传）
	code, err := chReader.UVarInt()
	if err != nil {
		t.Fatalf("read server response: %v", err)
	}

	if proto.ServerCode(code) == proto.ServerCodeHello {
		serverName, _ := chReader.Str()
		t.Logf("Proxy passthrough OK: connected to %s", serverName)
	} else {
		t.Logf("Server code: %d (may need auth configuration)", code)
	}
	fmt.Println("Integration test completed")
}
