package proxy

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"regexp"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	log "sentioxyz/sentio-core/common/log"
	registryProtos "sentioxyz/sentio-core/service/database_registry/protos"
)

// createUserDatabaseTimeout bounds the full round trip: dial the picked
// indexer's sentio-node, submit createUserDatabase, wait for the tx to mine.
// The contract call itself waits up to ~60s inside the server; we add
// generous slack for dial + network.
const createUserDatabaseTimeout = 90 * time.Second

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

// handleCreateDatabase intercepts a `CREATE DATABASE <name>` query. It picks
// a random active indexer from the state mirror, asks that indexer's
// sentio-node to submit the on-chain createUserDatabase tx, and writes an
// EndOfStream packet back to the client when the tx is mined.
//
// Returns true if the query was intercepted (caller must stop forwarding).
// On any error it sends a ClickHouse Exception packet to the client and
// still returns true — the intercept is terminal either way.
func (p *Proxy) handleCreateDatabase(ctx context.Context, clientConn net.Conn, id int64, dbName, userAddr string) bool {
	if userAddr == "" {
		log.Infof("[conn %d] create_database: rejected — client is not authenticated (no signer)", id)
		sendExceptionToClient(clientConn, 497, "ACCESS_DENIED",
			"CREATE DATABASE requires a signed request (x_auth_token setting missing)")
		return true
	}

	target, err := p.pickIndexerForCreateDatabase()
	if err != nil {
		log.Errorf("[conn %d] create_database: no indexer available: %v", id, err)
		sendExceptionToClient(clientConn, 999, "PROXY_ERROR",
			fmt.Sprintf("No indexer available to create database: %v", err))
		return true
	}
	log.Infof("[conn %d] create_database: routing db=%q user=%s to indexer sentio-node %s", id, dbName, userAddr, target)

	callCtx, cancel := context.WithTimeout(ctx, createUserDatabaseTimeout)
	defer cancel()

	if err := callCreateUserDatabase(callCtx, target, dbName, userAddr); err != nil {
		log.Errorf("[conn %d] create_database: onchain create failed db=%q user=%s target=%s: %v", id, dbName, userAddr, target, err)
		sendExceptionToClient(clientConn, 1004, "CREATE_DATABASE_FAILED",
			fmt.Sprintf("CREATE DATABASE %s: %v", dbName, err))
		return true
	}

	log.Infof("[conn %d] create_database: db=%q registered onchain (owner=%s, via=%s)", id, dbName, userAddr, target)
	sendEndOfStreamToClient(clientConn)
	return true
}

// pickIndexerForCreateDatabase returns the sentio-node gRPC address of a
// randomly chosen active indexer. Unlike pickRandomBoundProxy (which is
// for client-facing CH forwarding and excludes self to avoid loops), this
// function includes self in the pool: for CREATE DATABASE we want any
// indexer to be eligible so allocation spreads evenly across the network,
// and calling our own local sentio-node is the correct behavior when self
// wins the draw.
func (p *Proxy) pickIndexerForCreateDatabase() (string, error) {
	if p.networkState == nil {
		return "", fmt.Errorf("network state not configured")
	}
	infos := p.networkState.GetAllIndexerInfos()
	if len(infos) == 0 {
		return "", fmt.Errorf("no indexers found in network state")
	}
	candidates := make([]string, 0, len(infos))
	for _, info := range infos {
		if addr := sentioNodeAddrFor(info); addr != "" {
			candidates = append(candidates, addr)
		}
	}
	if len(candidates) == 0 {
		return "", fmt.Errorf("no indexers expose a compute-node RPC port")
	}
	return candidates[rand.Intn(len(candidates))], nil
}

// sentioNodeAddrFor returns the host:port to dial for an indexer's
// sentio-node gRPC server, or "" if the indexer has not advertised one.
func sentioNodeAddrFor(info IndexerInfo) string {
	if info.IndexerUrl == "" || info.ComputeNodeRpcPort == 0 {
		return ""
	}
	return fmt.Sprintf("%s:%d", info.IndexerUrl, info.ComputeNodeRpcPort)
}

// callCreateUserDatabase dials the given sentio-node address and submits a
// single CreateUserDatabase RPC. Connection is closed before return; this
// path is low-frequency (one dial per `CREATE DATABASE`) so a per-call
// dial keeps the code simple at acceptable cost.
func callCreateUserDatabase(ctx context.Context, sentioNodeAddr, dbID, userAddr string) error {
	conn, err := grpc.NewClient(sentioNodeAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial sentio-node %s: %w", sentioNodeAddr, err)
	}
	defer conn.Close()

	client := registryProtos.NewDatabaseRegistryServiceClient(conn)
	_, err = client.CreateUserDatabase(ctx, &registryProtos.CreateUserDatabaseRequest{
		DatabaseId:  dbID,
		UserAddress: userAddr,
	})
	if err != nil {
		return err
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

