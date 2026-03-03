package proxy

import (
	"bufio"
	"bytes"
	"net"
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

func TestParseRouteFromUser(t *testing.T) {
	tests := []struct {
		name       string
		user       string
		wantTarget string
		wantUser   string
		wantRoute  bool
	}{
		{
			name:       "valid route",
			user:       "__route__10.0.0.8:9001__default",
			wantTarget: "10.0.0.8:9001",
			wantUser:   "default",
			wantRoute:  true,
		},
		{
			name:       "valid route with hostname",
			user:       "__route__proxy2.example.com:9001__admin",
			wantTarget: "proxy2.example.com:9001",
			wantUser:   "admin",
			wantRoute:  true,
		},
		{
			name:      "no route prefix",
			user:      "default",
			wantRoute: false,
		},
		{
			name:      "empty user",
			user:      "",
			wantRoute: false,
		},
		{
			name:      "prefix only",
			user:      "__route__",
			wantRoute: false,
		},
		{
			name:      "prefix with addr but no second separator",
			user:      "__route__10.0.0.8:9001",
			wantRoute: false,
		},
		{
			name:      "prefix with addr and separator but empty user",
			user:      "__route__10.0.0.8:9001__",
			wantRoute: false,
		},
		{
			name:      "prefix with separator but empty addr",
			user:      "__route____default",
			wantRoute: false,
		},
		{
			name:       "user with underscores",
			user:       "__route__10.0.0.8:9001__my_user_name",
			wantTarget: "10.0.0.8:9001",
			wantUser:   "my_user_name",
			wantRoute:  true,
		},
		{
			name:       "user contains double underscore",
			user:       "__route__10.0.0.8:9001__user__extra",
			wantTarget: "10.0.0.8:9001",
			wantUser:   "user__extra",
			wantRoute:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target, user, isRoute := parseRouteFromUser(tt.user)
			if isRoute != tt.wantRoute {
				t.Fatalf("isRoute = %v, want %v", isRoute, tt.wantRoute)
			}
			if !tt.wantRoute {
				return
			}
			if target != tt.wantTarget {
				t.Errorf("targetAddr = %q, want %q", target, tt.wantTarget)
			}
			if user != tt.wantUser {
				t.Errorf("realUser = %q, want %q", user, tt.wantUser)
			}
		})
	}
}

func TestIsLocalConnection(t *testing.T) {
	if isLocalConnection(nil) {
		t.Error("nil conn should return false")
	}

	// Create a local TCP listener to get a real loopback connection
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	// Accept in background
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()

	// Dial to get a client-side loopback connection
	clientConn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()

	serverConn := <-accepted
	defer serverConn.Close()

	// serverConn.RemoteAddr() should be 127.0.0.1
	if !isLocalConnection(serverConn) {
		t.Errorf("loopback connection should be detected as local, got RemoteAddr=%s", serverConn.RemoteAddr())
	}
}

func TestRewriteHelloUser(t *testing.T) {
	// Build a Hello payload (without type byte) using proto.Buffer
	var payload proto.Buffer
	payload.PutString("TestClient")                      // client_name
	payload.PutUVarInt(22)                               // major
	payload.PutUVarInt(8)                                // minor
	payload.PutUVarInt(54460)                            // revision
	payload.PutString("default")                         // database
	payload.PutString("__route__10.0.0.8:9001__default") // user (with route)
	payload.PutString("secret123")                       // password

	rewritten, err := rewriteHelloUser(payload.Buf, "real_user")
	if err != nil {
		t.Fatalf("rewriteHelloUser failed: %v", err)
	}

	// Verify the rewritten Hello: parse it back
	if len(rewritten) == 0 {
		t.Fatal("rewritten Hello is empty")
	}
	// First byte should be type 0 (Hello)
	if rewritten[0] != byte(proto.ClientCodeHello) {
		t.Fatalf("type byte = %d, want 0", rewritten[0])
	}

	// Decode the rewritten Hello (skip type byte)
	var hello proto.ClientHello
	chReader := proto.NewReader(newTestBufReader(rewritten[1:]))
	if err := hello.Decode(chReader); err != nil {
		t.Fatalf("decode rewritten Hello: %v", err)
	}

	if hello.Name != "TestClient" {
		t.Errorf("client_name = %q, want %q", hello.Name, "TestClient")
	}
	if hello.ProtocolVersion != 54460 {
		t.Errorf("revision = %d, want %d", hello.ProtocolVersion, 54460)
	}
	if hello.Database != "default" {
		t.Errorf("database = %q, want %q", hello.Database, "default")
	}
	if hello.User != "real_user" {
		t.Errorf("user = %q, want %q", hello.User, "real_user")
	}
	if hello.Password != "secret123" {
		t.Errorf("password = %q, want %q", hello.Password, "secret123")
	}
}

func TestRewriteHelloUser_PreservesAllFields(t *testing.T) {
	// Build a Hello with different values
	var payload proto.Buffer
	payload.PutString("ClickHouse client")                  // client_name
	payload.PutUVarInt(24)                                  // major
	payload.PutUVarInt(3)                                   // minor
	payload.PutUVarInt(54471)                               // revision (latest)
	payload.PutString("mydb")                               // database
	payload.PutString("__route__192.168.1.100:9001__admin") // user
	payload.PutString("")                                   // password (empty)

	rewritten, err := rewriteHelloUser(payload.Buf, "admin")
	if err != nil {
		t.Fatalf("rewriteHelloUser failed: %v", err)
	}

	// Decode
	var hello proto.ClientHello
	chReader := proto.NewReader(newTestBufReader(rewritten[1:]))
	if err := hello.Decode(chReader); err != nil {
		t.Fatalf("decode: %v", err)
	}

	if hello.Name != "ClickHouse client" {
		t.Errorf("client_name = %q, want %q", hello.Name, "ClickHouse client")
	}
	if hello.Database != "mydb" {
		t.Errorf("database = %q, want %q", hello.Database, "mydb")
	}
	if hello.User != "admin" {
		t.Errorf("user = %q, want %q", hello.User, "admin")
	}
	if hello.Password != "" {
		t.Errorf("password = %q, want empty", hello.Password)
	}
}

func TestBuildRewriteMappingsRouteEncoding(t *testing.T) {
	// Create mock network state with a remote indexer
	state := NewInMemoryNetworkState()
	state.IndexerInfos[1] = IndexerInfo{
		IndexerId:           1,
		IndexerUrl:          "10.0.0.8",
		ClickhouseProxyPort: 9001,
	}
	state.ProcessorAllocations["coinbase"] = []ProcessorAllocation{
		{ProcessorId: "coinbase", IndexerId: 1},
	}
	state.ProcessorInfos["coinbase"] = ProcessorInfo{ProcessorId: "coinbase"}

	config := RewriterConfig{
		Enabled:  true,
		Upstream: "localhost:9000", // local CH — indexer addr differs, so it's remote
		Listen:   ":9001",
	}

	rewriter := &SentioNetworkRewriter{
		config:               config,
		networkState:         state,
		tableRewriterFactory: DefaultTableRewriterFactory("sentio"),
	}

	tables := []ParsedTable{
		{FullMatch: "sentio_coinbase.transfer", ProcessorId: "coinbase", TableName: "transfer"},
	}

	_, remoteMap := rewriter.buildRewriteMappings(nil, tables, "default", "password")

	if len(remoteMap) != 1 {
		t.Fatalf("expected 1 remote table, got %d", len(remoteMap))
	}

	rt, ok := remoteMap["sentio_coinbase.transfer"]
	if !ok {
		t.Fatal("expected sentio_coinbase.transfer in remote map")
	}

	// Verify addr is rewritten to localhost
	expectedAddr := "localhost:9001"
	if rt.Addr != expectedAddr {
		t.Errorf("Addr = %q, want %q", rt.Addr, expectedAddr)
	}

	// Verify user contains route encoding
	expectedUser := "__route__10.0.0.8:9001__default"
	if rt.User != expectedUser {
		t.Errorf("User = %q, want %q", rt.User, expectedUser)
	}

	// Verify password is passed through
	if rt.Password != "password" {
		t.Errorf("Password = %q, want %q", rt.Password, "password")
	}

	// Verify database is set
	if rt.Database != "sentio" {
		t.Errorf("Database = %q, want %q", rt.Database, "sentio")
	}
}

// newTestBufReader creates a bufio.Reader from bytes for test use.
func newTestBufReader(data []byte) *bufio.Reader {
	return bufio.NewReader(bytes.NewReader(data))
}
