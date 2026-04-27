package proxy

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	chgo "github.com/ClickHouse/clickhouse-go/v2"

	log "sentioxyz/sentio-core/common/log"
	registryProtos "sentioxyz/sentio-core/service/database_registry/protos"
)

// writePermission must match sentio-core's state.WritePermission — the
// string sentio-node's syncDatabaseWriters writes into the permission
// map for accounts authorized to perform write operations on a user DB.
const writePermission = "write"

// isDatabaseWriter reports whether addr is authorized to perform write
// operations (DROP, INSERT/UPDATE/DELETE, ALTER) on the given user
// database. Mirrors sentio-core's state.IsDatabaseWriter: signer is a
// writer iff it equals db.Owner OR the materialized permission map
// records perms[dbId] == "write" for that account. Address comparison
// is case-insensitive (state mirror values may be EIP-55 or lowercased
// depending on the producer).
func isDatabaseWriter(db DatabaseInfo, perms map[string]map[string]string, addr string) bool {
	if addr == "" {
		return false
	}
	if strings.EqualFold(db.Owner, addr) {
		return true
	}
	for account, p := range perms {
		if strings.EqualFold(account, addr) {
			return p[db.DatabaseId] == writePermission
		}
	}
	return false
}

// createUserDatabaseTimeout bounds the full round trip: dial the local
// sentio-node, submit createUserDatabase, wait for the tx to mine.
// The contract call itself waits up to ~60s inside the server; we add
// generous slack for dial + network.
const createUserDatabaseTimeout = 90 * time.Second

// forwardCreateDatabaseTimeout bounds a proxy-to-proxy forward: the remote
// proxy does the gRPC + onchain work, so we must cover its whole budget
// plus a CH handshake.
const forwardCreateDatabaseTimeout = 120 * time.Second

// sentioRoutedSettingKey is a custom ClickHouse setting that piggybacks on
// the SQL_-prefix auth-passthrough mechanism (see validator.AuthTokenSettingKey)
// to carry one bit of routing metadata across a proxy→proxy forward: non-empty
// value means "this Query was forwarded by another proxy, handle locally
// instead of forwarding again".
//
// User address is NOT carried as a setting — the user's JWS auth token is
// forwarded byte-for-byte alongside the byte-identical SQL body, so the
// receiver's EthValidator re-derives the same signer address that the
// sender did. Adding a separate user_address setting would be redundant.
const sentioRoutedSettingKey = "SQL_sentio_routed"

// createDatabaseRegexp matches `CREATE DATABASE <ident>` at the start of a
// statement. Only the bare form is recognized in this first cut — no
// IF NOT EXISTS, no ON CLUSTER, no ENGINE=... clauses. Anything more
// complex falls through to the upstream (which will reject it, since the
// physical ClickHouse database is not the user's to create).
var createDatabaseRegexp = regexp.MustCompile(`(?is)^\s*CREATE\s+DATABASE\s+` + "`?" + `([a-zA-Z0-9_]+)` + "`?" + `\s*;?\s*$`)

// isCreateDatabase reports whether sql is a bare `CREATE DATABASE <name>`
// statement and returns the database name. Case insensitive, tolerates
// surrounding whitespace, optional trailing semicolon, and backtick-quoted
// identifiers.
func isCreateDatabase(sql string) (string, bool) {
	m := createDatabaseRegexp.FindStringSubmatch(sql)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// dropDatabaseRegexp matches `DROP DATABASE <ident>` at the start of a
// statement, mirroring createDatabaseRegexp. IF EXISTS / ON CLUSTER /
// SYNC|ASYNC variants are intentionally NOT matched — the upstream will
// reject them and we can lift the restriction once the proxy needs to
// support more shapes.
var dropDatabaseRegexp = regexp.MustCompile(`(?is)^\s*DROP\s+DATABASE\s+` + "`?" + `([a-zA-Z0-9_]+)` + "`?" + `\s*;?\s*$`)

// isDropDatabase reports whether sql is a bare `DROP DATABASE <name>`
// statement and returns the database name.
func isDropDatabase(sql string) (string, bool) {
	m := dropDatabaseRegexp.FindStringSubmatch(sql)
	if m == nil {
		return "", false
	}
	return m[1], true
}

// handleCreateDatabase intercepts a `CREATE DATABASE <name>` query. Routing:
//
//  1. If the incoming query carries SQL_sentio_routed=1, this proxy is the
//     terminal node — call local sentio-node and return EndOfStream.
//  2. Otherwise pick a random active bound proxy (pickRandomBoundProxy
//     already excludes self). If one is available, forward the query over
//     a fresh ClickHouse connection to that proxy with SQL_sentio_routed=1
//     injected (the user's JWS auth token is passed through verbatim, so
//     the receiver's validator re-derives the same signer); stream its
//     response back.
//  3. If no remote proxy is available (single-indexer testnet or all
//     remotes degraded), fall back to local submission.
//
// Returns true in all branches — the intercept is terminal either way.
// On any error it sends a ClickHouse Exception to the client.
func (p *Proxy) handleCreateDatabase(ctx context.Context, clientConn net.Conn, id int64, originalSQL, dbName, userAddr string, settings map[string]string) bool {
	if userAddr == "" {
		log.Infof("[conn %d] create_database: rejected — client is not authenticated (no signer)", id)
		sendExceptionToClient(clientConn, 497, "ACCESS_DENIED",
			"CREATE DATABASE requires a signed request (x_auth_token setting missing)")
		return true
	}

	// Receiver branch: a forwarding proxy already picked us, do not forward again.
	// userAddr is the validator-recovered address from the forwarded JWS — the
	// same address the original sender saw, since SQL body and token bytes are
	// identical across the hop.
	if _, routed := settings[sentioRoutedSettingKey]; routed {
		log.Infof("[conn %d] create_database: terminal (routed) db=%q owner=%s", id, dbName, userAddr)
		p.submitCreateDatabaseLocal(ctx, clientConn, id, dbName, userAddr)
		return true
	}

	// Sender branch: try to forward to a random bound proxy.
	target, err := p.pickRandomBoundProxy()
	if err != nil {
		log.Infof("[conn %d] create_database: no remote proxy available (%v), handling locally", id, err)
		p.submitCreateDatabaseLocal(ctx, clientConn, id, dbName, userAddr)
		return true
	}

	authToken := settings[AuthTokenSettingKey]
	if authToken == "" {
		authToken = settings["x_auth_token"]
	}
	if authToken == "" {
		// Should never happen: validator already accepted the query, so a token must have been present.
		log.Errorf("[conn %d] create_database: missing auth token in settings, cannot forward", id)
		sendExceptionToClient(clientConn, 999, "PROXY_ERROR",
			"CREATE DATABASE: internal — auth token missing post-validation")
		return true
	}

	log.Infof("[conn %d] create_database: forwarding db=%q user=%s via %s", id, dbName, userAddr, target)
	if err := p.forwardCreateDatabase(ctx, target, originalSQL, dbName, authToken); err != nil {
		log.Errorf("[conn %d] create_database: forward to %s failed: %v", id, target, err)
		sendExceptionToClient(clientConn, 1004, "CREATE_DATABASE_FAILED",
			fmt.Sprintf("CREATE DATABASE %s: %v", dbName, err))
		return true
	}
	log.Infof("[conn %d] create_database: db=%q registered via %s (owner=%s)", id, dbName, target, userAddr)
	sendEndOfStreamToClient(clientConn)
	return true
}

// submitCreateDatabaseLocal calls the co-located sentio-node's
// DatabaseRegistryService over the existing UsageClient gRPC connection
// (the same listener serves both services). This is the terminal side of
// the flow — either we're the indexer chosen by a peer's forward, or
// there is no remote proxy and we fall back to self.
func (p *Proxy) submitCreateDatabaseLocal(ctx context.Context, clientConn net.Conn, id int64, dbName, userAddr string) {
	if p.usageClient == nil {
		log.Errorf("[conn %d] create_database: usage client not configured, no gRPC conn to local sentio-node", id)
		sendExceptionToClient(clientConn, 999, "PROXY_ERROR",
			"CREATE DATABASE: proxy has no gRPC channel to local sentio-node")
		return
	}

	callCtx, cancel := context.WithTimeout(ctx, createUserDatabaseTimeout)
	defer cancel()

	client := registryProtos.NewDatabaseRegistryServiceClient(p.usageClient.Conn())
	if _, err := client.CreateUserDatabase(callCtx, &registryProtos.CreateUserDatabaseRequest{
		DatabaseId:  dbName,
		UserAddress: userAddr,
	}); err != nil {
		log.Errorf("[conn %d] create_database: onchain create failed db=%q user=%s: %v", id, dbName, userAddr, err)
		sendExceptionToClient(clientConn, 1004, "CREATE_DATABASE_FAILED",
			fmt.Sprintf("CREATE DATABASE %s: %v", dbName, err))
		return
	}
	log.Infof("[conn %d] create_database: db=%q registered onchain (owner=%s)", id, dbName, userAddr)
	sendEndOfStreamToClient(clientConn)
}

// forwardCreateDatabase opens a fresh CH connection to the target proxy
// and replays the CREATE DATABASE query with SQL_sentio_routed=1 set so
// the receiver handles it locally instead of forwarding again. The user's
// JWS auth token is passed through verbatim and the SQL is forwarded
// byte-for-byte (originalSQL == eq.Body from the user's Query packet);
// byte equality is required because the token signs keccak256 of the SQL
// body, and it lets the receiver's EthValidator re-derive the same signer
// address the sender saw. No relay signer involved.
func (p *Proxy) forwardCreateDatabase(ctx context.Context, target, originalSQL, dbName, authToken string) error {
	callCtx, cancel := context.WithTimeout(ctx, forwardCreateDatabaseTimeout)
	defer cancel()

	conn, err := chgo.Open(&chgo.Options{
		Addr: []string{target},
		Auth: chgo.Auth{Username: "default"},
		Settings: chgo.Settings{
			AuthTokenSettingKey:    authToken,
			sentioRoutedSettingKey: "1",
		},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("open forward conn to %s: %w", target, err)
	}
	defer conn.Close()

	if err := conn.Exec(callCtx, originalSQL); err != nil {
		return fmt.Errorf("exec forwarded CREATE DATABASE on %s: %w", target, err)
	}
	return nil
}

// sendEndOfStreamToClient writes a single-byte ServerCodeEndOfStream(5)
// packet, which ClickHouse clients treat as "server accepted the query and
// has nothing more to say." Matches the sendExceptionToClient helper but
// for the success path when the proxy terminates the query locally.
func sendEndOfStreamToClient(conn net.Conn) {
	buf := &proto.Buffer{}
	proto.ServerCodeEndOfStream.Encode(buf)
	_, _ = conn.Write(buf.Buf)
}

// handleDropDatabase intercepts a `DROP DATABASE <name>` query. Routing
// differs from CREATE: every user database is bound on-chain to a
// specific indexer (DatabaseInfo.IndexerId), and only that indexer's
// sentio-node should send the deleteDatabase tx. Decisions:
//
//  1. signerAddr empty → 497 ACCESS_DENIED.
//  2. SQL_sentio_routed=1 set → terminal, call local sentio-node and
//     return EndOfStream.
//  3. state.GetDatabase missing → 81 UNKNOWN_DATABASE.
//  4. signer is not Owner and lacks "write" permission → 497.
//  5. owner indexer is self → terminal local call.
//  6. owner indexer offline / no proxy port → 999 PROXY_ERROR.
//  7. else forward to the owner indexer's proxy.
//
// On any error this method writes an Exception to the client; success
// writes EndOfStream. Always terminal.
func (p *Proxy) handleDropDatabase(ctx context.Context, clientConn net.Conn, id int64, originalSQL, dbName, userAddr string, settings map[string]string) bool {
	if userAddr == "" {
		log.Infof("[conn %d] drop_database: rejected — client is not authenticated (no signer)", id)
		sendExceptionToClient(clientConn, 497, "ACCESS_DENIED",
			"DROP DATABASE requires a signed request (x_auth_token setting missing)")
		return true
	}

	// Receiver branch: a forwarding proxy already picked us, do not forward again.
	// userAddr is the validator-recovered address from the forwarded JWS.
	if _, routed := settings[sentioRoutedSettingKey]; routed {
		log.Infof("[conn %d] drop_database: terminal (routed) db=%q owner=%s", id, dbName, userAddr)
		p.submitDeleteDatabaseLocal(ctx, clientConn, id, dbName, userAddr)
		return true
	}

	// Sender branch: look up the bound indexer in state.
	if p.networkState == nil {
		log.Errorf("[conn %d] drop_database: network state not configured", id)
		sendExceptionToClient(clientConn, 999, "PROXY_ERROR",
			"DROP DATABASE: proxy has no network state")
		return true
	}
	db, ok := p.networkState.GetDatabase(dbName)
	if !ok {
		log.Infof("[conn %d] drop_database: unknown database %q", id, dbName)
		sendExceptionToClient(clientConn, 81, "UNKNOWN_DATABASE",
			fmt.Sprintf("Database %q does not exist", dbName))
		return true
	}
	if !isDatabaseWriter(db, p.networkState.GetDatabasePermissions(), userAddr) {
		log.Infof("[conn %d] drop_database: %s is not authorized to drop %q (owner=%s)", id, userAddr, dbName, db.Owner)
		sendExceptionToClient(clientConn, 497, "ACCESS_DENIED",
			fmt.Sprintf("address %s is not authorized to drop database %q", userAddr, dbName))
		return true
	}

	target, isLocal, err := p.pickProxyForDatabase(db)
	if err != nil {
		log.Errorf("[conn %d] drop_database: routing for db=%q failed: %v", id, dbName, err)
		sendExceptionToClient(clientConn, 999, "PROXY_ERROR",
			fmt.Sprintf("DROP DATABASE %s: %v", dbName, err))
		return true
	}
	if isLocal {
		log.Infof("[conn %d] drop_database: terminal (local) db=%q owner=%s indexer=%d", id, dbName, userAddr, db.IndexerId)
		p.submitDeleteDatabaseLocal(ctx, clientConn, id, dbName, userAddr)
		return true
	}

	authToken := settings[AuthTokenSettingKey]
	if authToken == "" {
		authToken = settings["x_auth_token"]
	}
	if authToken == "" {
		log.Errorf("[conn %d] drop_database: missing auth token in settings, cannot forward", id)
		sendExceptionToClient(clientConn, 999, "PROXY_ERROR",
			"DROP DATABASE: internal — auth token missing post-validation")
		return true
	}

	log.Infof("[conn %d] drop_database: forwarding db=%q user=%s via %s (indexer=%d)", id, dbName, userAddr, target, db.IndexerId)
	if err := p.forwardDropDatabase(ctx, target, originalSQL, dbName, authToken); err != nil {
		log.Errorf("[conn %d] drop_database: forward to %s failed: %v", id, target, err)
		sendExceptionToClient(clientConn, 1004, "DROP_DATABASE_FAILED",
			fmt.Sprintf("DROP DATABASE %s: %v", dbName, err))
		return true
	}
	log.Infof("[conn %d] drop_database: db=%q deleted via %s (owner=%s)", id, dbName, target, userAddr)
	sendEndOfStreamToClient(clientConn)
	return true
}

// submitDeleteDatabaseLocal calls the co-located sentio-node's
// DatabaseRegistryService.DeleteUserDatabase over the existing
// UsageClient gRPC connection. Mirror of submitCreateDatabaseLocal — see
// that function for the shared rationale on connection reuse.
func (p *Proxy) submitDeleteDatabaseLocal(ctx context.Context, clientConn net.Conn, id int64, dbName, userAddr string) {
	if p.usageClient == nil {
		log.Errorf("[conn %d] drop_database: usage client not configured, no gRPC conn to local sentio-node", id)
		sendExceptionToClient(clientConn, 999, "PROXY_ERROR",
			"DROP DATABASE: proxy has no gRPC channel to local sentio-node")
		return
	}

	callCtx, cancel := context.WithTimeout(ctx, createUserDatabaseTimeout)
	defer cancel()

	client := registryProtos.NewDatabaseRegistryServiceClient(p.usageClient.Conn())
	if _, err := client.DeleteUserDatabase(callCtx, &registryProtos.DeleteUserDatabaseRequest{
		DatabaseId:  dbName,
		UserAddress: userAddr,
	}); err != nil {
		log.Errorf("[conn %d] drop_database: onchain delete failed db=%q user=%s: %v", id, dbName, userAddr, err)
		sendExceptionToClient(clientConn, 1004, "DROP_DATABASE_FAILED",
			fmt.Sprintf("DROP DATABASE %s: %v", dbName, err))
		return
	}
	log.Infof("[conn %d] drop_database: db=%q deleted onchain (requested by %s)", id, dbName, userAddr)
	sendEndOfStreamToClient(clientConn)
}

// forwardDropDatabase forwards a DROP DATABASE query to a specific peer
// proxy with sentio_routed=1 set. The user's original auth token is
// passed through verbatim and originalSQL is forwarded byte-for-byte
// (clickhouse-go's conn.Exec preserves the SQL body), so the receiver's
// EthValidator validates the token against the same SQL bytes the user
// signed and recovers the same signer address.
func (p *Proxy) forwardDropDatabase(ctx context.Context, target, originalSQL, dbName, authToken string) error {
	callCtx, cancel := context.WithTimeout(ctx, forwardCreateDatabaseTimeout)
	defer cancel()

	conn, err := chgo.Open(&chgo.Options{
		Addr: []string{target},
		Auth: chgo.Auth{Username: "default"},
		Settings: chgo.Settings{
			AuthTokenSettingKey:    authToken,
			sentioRoutedSettingKey: "1",
		},
		DialTimeout: 10 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("open forward conn to %s: %w", target, err)
	}
	defer conn.Close()

	if err := conn.Exec(callCtx, originalSQL); err != nil {
		return fmt.Errorf("exec forwarded DROP DATABASE on %s: %w", target, err)
	}
	return nil
}

// pickProxyForDatabase resolves the proxy target for a user database
// based on its on-chain IndexerId. Returns isLocal=true if the bound
// indexer is this proxy itself (the caller should handle the request
// locally instead of forwarding). Errors when the bound indexer is
// missing from state or has no clickhouse proxy port advertised.
func (p *Proxy) pickProxyForDatabase(db DatabaseInfo) (string, bool, error) {
	infos := p.networkState.GetAllIndexerInfos()
	info, ok := infos[db.IndexerId]
	if !ok {
		return "", false, fmt.Errorf("owner indexer %d not in state mirror", db.IndexerId)
	}
	if info.ClickhouseProxyPort == 0 {
		return "", false, fmt.Errorf("owner indexer %d has no clickhouse proxy port", db.IndexerId)
	}
	addr := fmt.Sprintf("%s:%d", info.IndexerUrl, info.ClickhouseProxyPort)
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", false, fmt.Errorf("parse owner proxy address %q: %w", addr, err)
	}
	return addr, isLocalAddress(host), nil
}
