package proxy

// credential_provider.go — CredentialProvider interface and ckhmanager-backed implementation.
//
// When credential_replace_enabled is true, the proxy uses CredentialProvider to
// replace the client's user/password with the real ClickHouse credentials from
// CkhManagerConfigPath before forwarding to upstream ClickHouse.
//
// This allows sidecar clients to connect without knowing the real ClickHouse password;
// they only need to provide a JWS token for authentication.

import (
	"fmt"

	ckhmanager "sentioxyz/sentio-core/common/clickhousemanager"
	log "sentioxyz/sentio-core/common/log"
)

// CredentialProvider provides ClickHouse credentials for upstream connections.
type CredentialProvider interface {
	// GetDefaultCredential returns the default credential for the local upstream ClickHouse.
	// Used during Hello handshake to replace the client's user/password.
	GetDefaultCredential() (username, password string, err error)

	// GetCredentialForIndexer returns the credential for a specific indexer's ClickHouse.
	// Used for remote() function calls targeting other indexers.
	GetCredentialForIndexer(indexerInfo IndexerInfo) (username, password string, err error)
}

// ckhManagerCredentialProvider implements CredentialProvider using ckhmanager.Manager.
type ckhManagerCredentialProvider struct {
	ckhMgr        ckhmanager.Manager
	privateKeyHex string // Ethereum private key for proxy auth (passed to WithPrivateKeyHex)
}

// NewCkhManagerCredentialProvider creates a CredentialProvider backed by ckhmanager.Manager.
// privateKeyHex is the relay private key used for proxy-to-proxy auth (may be empty).
func NewCkhManagerCredentialProvider(ckhMgr ckhmanager.Manager, privateKeyHex string) CredentialProvider {
	return &ckhManagerCredentialProvider{
		ckhMgr:        ckhMgr,
		privateKeyHex: privateKeyHex,
	}
}

// GetDefaultCredential returns the admin credential from the default shard.
// It uses AdminRole (empty string) which maps to the "subgraph" credential key
// in the ckhmanager config (e.g. credential["subgraph"] = {username, password, database}).
func (p *ckhManagerCredentialProvider) GetDefaultCredential() (string, string, error) {
	defaultIndex := p.ckhMgr.DefaultIndex()
	shard := p.ckhMgr.GetShardByIndex(defaultIndex)
	if shard == nil {
		return "", "", fmt.Errorf("default shard (index %d) not found", defaultIndex)
	}

	options := []func(*ckhmanager.ShardingParameter){
		ckhmanager.WithUnderlyingProxy(true),
		ckhmanager.WithRole(ckhmanager.AdminRole),
	}
	if p.privateKeyHex != "" {
		options = append(options, ckhmanager.WithPrivateKeyHex(p.privateKeyHex))
	}

	username, password, _, _, err := shard.GetConnInfo(options...)
	if err != nil {
		return "", "", fmt.Errorf("get default credential: %w", err)
	}

	log.Debugf("credential_provider: default credential resolved, user=%s", username)
	return username, password, nil
}

// GetCredentialForIndexer returns the admin credential for a specific indexer's ClickHouse.
// It creates a temporary shard via NewShardByStateIndexer and retrieves the admin credential.
func (p *ckhManagerCredentialProvider) GetCredentialForIndexer(indexerInfo IndexerInfo) (string, string, error) {
	shard := p.ckhMgr.NewShardByStateIndexer(indexerInfo)
	if shard == nil {
		return "", "", fmt.Errorf("failed to create shard for indexer %d", indexerInfo.IndexerId)
	}

	options := []func(*ckhmanager.ShardingParameter){
		ckhmanager.WithUnderlyingProxy(true),
		ckhmanager.WithRole(ckhmanager.AdminRole),
	}
	if p.privateKeyHex != "" {
		options = append(options, ckhmanager.WithPrivateKeyHex(p.privateKeyHex))
	}

	username, password, _, _, err := shard.GetConnInfo(options...)
	if err != nil {
		return "", "", fmt.Errorf("get credential for indexer %d: %w", indexerInfo.IndexerId, err)
	}

	log.Debugf("credential_provider: indexer %d credential resolved, user=%s", indexerInfo.IndexerId, username)
	return username, password, nil
}
