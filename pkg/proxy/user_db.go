package proxy

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	chgo "github.com/ClickHouse/clickhouse-go/v2"

	log "sentioxyz/sentio-core/common/log"
	registryProtos "sentioxyz/sentio-core/service/database_registry/protos"
)

// createUserDatabaseTimeout bounds the full round trip: dial the local
// sentio-node, submit createUserDatabase, wait for the tx to mine.
// The contract call itself waits up to ~60s inside the server; we add
// generous slack for dial + network.
const createUserDatabaseTimeout = 90 * time.Second

// forwardCreateDatabaseTimeout bounds a proxy-to-proxy forward: the remote
// proxy does the gRPC + onchain work, so we must cover its whole budget
// plus a CH handshake.
const forwardCreateDatabaseTimeout = 120 * time.Second

// Custom ClickHouse settings used to carry routing metadata across a
// proxy→proxy forward of `CREATE DATABASE`. Setting keys that start with
// `SQL_` (validator.AuthTokenSettingKey) are already reserved for auth
// passthrough; these two piggyback on the same passthrough mechanism.
//
// sentioRoutedSettingKey: non-empty value means "this Query was forwarded
// by another proxy, handle locally instead of forwarding again".
//
// sentioUserAddressSettingKey: lower-case 0x-prefixed address of the
// original end-user signer. Needed because the forwarded Query is
// authenticated by the forwarding proxy's relay signer, not the user —
// so the receiver can't re-derive user_address from the auth token.
const (
	sentioRoutedSettingKey      = "SQL_sentio_routed"
	sentioUserAddressSettingKey = "SQL_sentio_user_address"
)

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

// handleCreateDatabase intercepts a `CREATE DATABASE <name>` query. Routing:
//
//  1. If the incoming query carries SQL_sentio_routed=1, this proxy is the
//     terminal node — call local sentio-node and return EndOfStream.
//  2. Otherwise pick a random active bound proxy (pickRandomBoundProxy
//     already excludes self). If one is available, forward the query over
//     a fresh ClickHouse connection to that proxy with SQL_sentio_routed=1
//     + SQL_sentio_user_address=<user> injected; stream its response back.
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
	if _, routed := settings[sentioRoutedSettingKey]; routed {
		owner := settings[sentioUserAddressSettingKey]
		if owner == "" {
			log.Errorf("[conn %d] create_database: routed query missing %s setting", id, sentioUserAddressSettingKey)
			sendExceptionToClient(clientConn, 999, "PROXY_ERROR",
				"forwarded CREATE DATABASE missing user_address")
			return true
		}
		log.Infof("[conn %d] create_database: terminal (routed) db=%q owner=%s", id, dbName, owner)
		p.submitCreateDatabaseLocal(ctx, clientConn, id, dbName, owner)
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
	if err := p.forwardCreateDatabase(ctx, target, originalSQL, dbName, userAddr, authToken); err != nil {
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
// and replays the CREATE DATABASE query with SQL_sentio_routed=1 +
// SQL_sentio_user_address=<userAddr> so the receiver handles it locally
// and records the correct owner. The original user's JWS auth token is
// passed through verbatim, and the SQL is forwarded byte-for-byte
// (originalSQL == eq.Body from the user's Query packet). Byte equality
// is required because the user's token signs keccak256 of the SQL body;
// any normalization (case, backticks, whitespace) would break the
// receiver's hash check. No relay signer required, which matches
// production where relay_private_key_hex is unset.
func (p *Proxy) forwardCreateDatabase(ctx context.Context, target, originalSQL, dbName, userAddr, authToken string) error {
	callCtx, cancel := context.WithTimeout(ctx, forwardCreateDatabaseTimeout)
	defer cancel()

	conn, err := chgo.Open(&chgo.Options{
		Addr: []string{target},
		Auth: chgo.Auth{Username: "default"},
		Settings: chgo.Settings{
			AuthTokenSettingKey:         authToken,
			sentioRoutedSettingKey:      "1",
			sentioUserAddressSettingKey: userAddr,
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
