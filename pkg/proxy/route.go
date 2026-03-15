package proxy

// route.go — Subquery route helpers for dynamic upstream routing.
//
// Plan B (user parameter route encoding):
// remote() 的 user 参数编码路由信息 "__route__<target_proxy_addr>__<real_user>",
// Proxy 在 Hello 阶段解析 user 字段，提取目标地址，直连目标 Proxy2。

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"strings"

	"github.com/ClickHouse/ch-go/proto"
)

const routePrefix = "__route__"

// parseRouteFromUser parses route information from the user parameter.
// Format: __route__<target_addr>__<real_user>
// Returns the target proxy address, the real user name, and whether routing was detected.
func parseRouteFromUser(user string) (targetAddr, realUser string, isRoute bool) {
	if !strings.HasPrefix(user, routePrefix) {
		return "", "", false
	}
	rest := user[len(routePrefix):] // e.g. "10.0.0.8:9001__default"
	idx := strings.Index(rest, "__")
	if idx < 0 {
		return "", "", false
	}
	targetAddr = rest[:idx]         // e.g. "10.0.0.8:9001"
	realUser = rest[idx+len("__"):] // e.g. "default"
	if targetAddr == "" || realUser == "" {
		return "", "", false
	}
	return targetAddr, realUser, true
}

// isLocalConnection checks if conn originates from the local machine.
// Allows loopback (127.0.0.1, ::1) and any IP assigned to local interfaces.
// Only local connections are allowed to use __route__ routing to prevent SSRF attacks.
func isLocalConnection(conn net.Conn) bool {
	if conn == nil {
		return false
	}
	addr, ok := conn.RemoteAddr().(*net.TCPAddr)
	if !ok {
		return false
	}
	if addr.IP.IsLoopback() {
		return true
	}
	// Check if the IP belongs to a local network interface
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, a := range addrs {
		if ipNet, ok := a.(*net.IPNet); ok && ipNet.IP.Equal(addr.IP) {
			return true
		}
	}
	return false
}

// rewriteHelloUser re-encodes a ClientHello packet, replacing the user field.
// Takes the raw Hello bytes (excluding type byte) and the real user to substitute.
// Returns the full Hello packet bytes including the type byte (0x00).
//
// ClientHello format:
//
//	[type=0x00] [client_name:String] [major:UVarInt] [minor:UVarInt]
//	[revision:UVarInt] [database:String] [user:String] [password:String]
func rewriteHelloUser(helloPayload []byte, realUser string) ([]byte, error) {
	// Decode original Hello fields
	r := proto.NewReader(bufio.NewReader(bytes.NewReader(helloPayload)))

	clientName, err := r.Str()
	if err != nil {
		return nil, fmt.Errorf("decode client_name: %w", err)
	}
	major, err := r.UVarInt()
	if err != nil {
		return nil, fmt.Errorf("decode major: %w", err)
	}
	minor, err := r.UVarInt()
	if err != nil {
		return nil, fmt.Errorf("decode minor: %w", err)
	}
	revision, err := r.UVarInt()
	if err != nil {
		return nil, fmt.Errorf("decode revision: %w", err)
	}
	database, err := r.Str()
	if err != nil {
		return nil, fmt.Errorf("decode database: %w", err)
	}
	// Skip original user (contains __route__ prefix)
	if _, err := r.Str(); err != nil {
		return nil, fmt.Errorf("decode user: %w", err)
	}
	password, err := r.Str()
	if err != nil {
		return nil, fmt.Errorf("decode password: %w", err)
	}

	// Re-encode with real user
	var buf proto.Buffer
	buf.PutByte(byte(proto.ClientCodeHello)) // type = 0x00
	buf.PutString(clientName)
	buf.PutUVarInt(major)
	buf.PutUVarInt(minor)
	buf.PutUVarInt(revision)
	buf.PutString(database)
	buf.PutString(realUser) // replaced user
	buf.PutString(password)

	return buf.Buf, nil
}
