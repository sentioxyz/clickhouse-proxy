package proxy

import (
	ckhmanager "sentioxyz/sentio-core/common/clickhousemanager"
	log "sentioxyz/sentio-core/common/log"
)

// LoadCKHManager creates a ckhmanager.Manager from a config file (YAML or JSON).
// The config file should contain ClickHouse credentials and connection settings.
//
// Example YAML config:
//
//	credential:
//	  admin:
//	    username: sentio
//	    password: "secret"
//	    database: sentio
//	dial_timeout: 5s
//	read_timeout: 30s
//	max_idle_connections: 5
//	max_open_connections: 10
//
// The Manager uses these credentials to connect to ClickHouse instances
// identified by IndexerInfo from NetworkState, for metadata queries
// (e.g., querying system.tables to discover physical table names).
func LoadCKHManager(configPath string) ckhmanager.Manager {
	mgr := ckhmanager.LoadManager(configPath)
	if mgr == nil {
		log.Fatalf("failed to load ckhmanager from config: %s", configPath)
	}
	log.Infof("ckhmanager loaded from %s", configPath)
	return mgr
}
