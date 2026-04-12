package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	ckhmanager "sentioxyz/sentio-core/common/clickhousemanager"
	log "sentioxyz/sentio-core/common/log"
	"sentioxyz/sentio-core/network/sqlrewriter"

	"github.com/sentioxyz/clickhouse-proxy/pkg/cluster"
	proxy "github.com/sentioxyz/clickhouse-proxy/pkg/proxy"
)

func main() {
	configPath := flag.String("config", envOrDefault("CK_CONFIG", ""), "path to JSON config file (optional)")
	flag.Parse()

	cfg := proxy.LoadConfig(*configPath)
	log.Infof("clickhouse-proxy starting. listen=%s upstream=%s dial_timeout=%s idle_timeout=%s stats_interval=%s log_queries=%t log_data=%t auth_enabled=%t",
		cfg.Listen, cfg.Upstream, cfg.DialTimeout, cfg.IdleTimeout, cfg.StatsInterval, cfg.LogQueries, cfg.LogData, cfg.AuthEnabled)

	// Detect forwarding-only mode: no local ClickHouse instance bound and no shard config
	if cfg.Upstream == "" && cfg.Shard == nil && !cfg.SidecarMode {
		cfg.ForwardingOnly = true
		log.Infof("forwarding-only mode: no upstream/shard configured, requests will be forwarded to bound proxies via NetworkState")
	}
	if cfg.Shard != nil && cfg.Upstream != "" {
		log.Warnf("both 'shard' and 'upstream' configured; 'shard' takes priority, 'upstream' will be ignored for routing")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		// R7-3: Prevent metrics HTTP server panic from crashing the whole process
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("metrics server panic recovered: %v", r)
			}
		}()
		log.Infof("metrics listening on %s", cfg.MetricsListen)
		if err := http.ListenAndServe(cfg.MetricsListen, promhttp.Handler()); err != nil {
			log.Infof("metrics server error: %v", err)
		}
	}()

	// ========== Sidecar mode: simplified startup ==========
	if cfg.SidecarMode {
		if cfg.SidecarPrivateKeyHex == "" {
			log.Fatalf("sidecar_mode requires sidecar_private_key_hex")
		}
		if cfg.SidecarUpstream == "" {
			log.Fatalf("sidecar_mode requires sidecar_upstream")
		}

		signer, err := proxy.NewRelaySigner(cfg.SidecarPrivateKeyHex)
		if err != nil {
			log.Fatalf("failed to create sidecar signer: %v", err)
		}

		p := proxy.NewProxy(cfg, nil, nil) // no validator, no rewriter
		p.SetSidecarSigner(signer)

		log.Infof("sidecar proxy mode: signing queries with address=%s, forwarding to %s",
			signer.Address(), cfg.SidecarUpstream)
		if err := p.Serve(ctx); err != nil {
			log.Fatalf("proxy stopped: %v", err)
		}
		return
	}

	// ========== Server mode: full initialization ==========

	// Create validator based on configuration
	var validator proxy.Validator
	if cfg.AuthEnabled {
		validator = proxy.NewEthValidator(cfg.AuthAllowedAddresses, cfg.AuthMaxTokenAge.Duration, true, cfg.AuthAllowNoAuth)
		log.Infof("Ethereum signature auth enabled with %d allowed addresses, allow_no_auth=%t", len(cfg.AuthAllowedAddresses), cfg.AuthAllowNoAuth)
	}

	// Create rewriter based on configuration
	var rewriter proxy.Rewriter
	// Initialize shared Redis client
	if cfg.NetworkStateRedis == "" {
		log.Fatalf("network_state_redis is required")
	}
	redisClient := redis.NewClient(&redis.Options{Addr: cfg.NetworkStateRedis})
	defer redisClient.Close()
	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		log.Fatalf("failed to connect to Redis at %s: %v", cfg.NetworkStateRedis, err)
	}

	// Load network state
	var networkState proxy.NetworkState
	state, err := proxy.NewRedisNetworkState(redisClient)
	if err != nil {
		log.Fatalf("failed to initialize Redis network state: %v", err)
	}
	networkState = state

	// Create rewriter (optional in forwarding-only mode)
	if !cfg.ForwardingOnly {
		// ClickHouse manager config is required for table mapping
		if cfg.CkhManagerConfigPath == "" {
			log.Fatalf("ckh_manager_config_path is required for SQL rewriter table mapping")
		}
		ckhMgr := ckhmanager.LoadManager(cfg.CkhManagerConfigPath)
		if ckhMgr == nil {
			log.Fatalf("failed to load ClickHouse manager from %s", cfg.CkhManagerConfigPath)
		}
		privateKeyHex := cfg.RelayPrivateKeyHex

		// Create table rewriter factory backed by sentio-core TableMapper
		tableRewriterFactory := func(ctx context.Context, processorId string,
			indexerInfo proxy.IndexerInfo, processorInfo proxy.ProcessorInfo) (proxy.SentioNetworkTableRewriter, error) {
			return sqlrewriter.NewTableMapper(privateKeyHex, processorId, ckhMgr, indexerInfo, processorInfo)
		}
		log.Infof("using sentio-core TableMapper, ckh_manager_config=%s", cfg.CkhManagerConfigPath)

		// Create rewriter
		rwConfig := proxy.RewriterConfig{
			Enabled:     true,
			ServiceAddr: cfg.RewriterServiceAddr,
			Upstream:    cfg.Upstream,
			Listen:      cfg.Listen,
			Timeout:     cfg.RewriterTimeout.Duration,
		}
		rw, err := proxy.NewSentioNetworkRewriter(rwConfig, networkState, tableRewriterFactory)
		if err != nil {
			log.Warnf("failed to create rewriter: %v, rewriting disabled", err)
		} else {
			rewriter = rw
			defer rw.Close()
			log.Infof("SQL rewriter enabled, service_addr=%s upstream=%s", cfg.RewriterServiceAddr, cfg.Upstream)
		}
	} else {
		log.Infof("forwarding-only mode: SQL rewriter disabled")
	}

	// Initialize cluster manager for multi-replica routing
	var clusterMgr *cluster.Manager
	if cfg.Shard != nil {
		// New multi-replica mode: use shard config
		var err error
		clusterMgr, err = cluster.NewManager(*cfg.Shard, cfg.HealthCheck, cfg.Pool, cfg.Routing)
		if err != nil {
			log.Fatalf("failed to create cluster manager: %v", err)
		}
		clusterMgr.Start(ctx)
		defer clusterMgr.Close()
		log.Infof("cluster manager started for shard %q with %d replicas", cfg.Shard.Name, len(cfg.Shard.Replicas))
	} else if cfg.Upstream != "" {
		// Backward-compatible mode: single upstream auto-wrapped as single-replica shard
		var err error
		clusterMgr, err = cluster.NewSingleReplicaManager(cfg.Upstream)
		if err != nil {
			log.Warnf("failed to create single-replica cluster manager for upstream %s: %v, falling back to direct dial", cfg.Upstream, err)
		} else {
			clusterMgr.Start(ctx)
			defer clusterMgr.Close()
			log.Infof("cluster manager started in single-replica mode for upstream %s", cfg.Upstream)
		}
	}

	p := proxy.NewProxy(cfg, validator, rewriter)
	p.SetNetworkState(networkState)
	p.SetClusterManager(clusterMgr)

	// Wire cluster manager to rewriter for multi-replica isLocal detection
	if clusterMgr != nil {
		if rw, ok := rewriter.(*proxy.SentioNetworkRewriter); ok && rw != nil {
			rw.SetClusterManager(clusterMgr)
		}
	}

	// Initialize relay JWS signer for proxy-to-proxy (__route__) token propagation
	if cfg.RelayPrivateKeyHex != "" {
		signer, err := proxy.NewRelaySigner(cfg.RelayPrivateKeyHex)
		if err != nil {
			log.Fatalf("failed to create relay signer: %v", err)
		}
		p.SetRelaySigner(signer)
		log.Infof("relay JWS signer enabled, address=%s", signer.Address())
	}

	// Initialize query usage client for billing integration
	if cfg.QueryUsageEnabled && cfg.SentioNodeAddr != "" {
		usageClient, err := proxy.NewUsageClient(cfg.SentioNodeAddr, redisClient)
		if err != nil {
			log.Warnf("failed to create usage client: %v, query billing disabled", err)
		} else {
			p.SetUsageClient(usageClient)
			defer usageClient.Close()
			log.Infof("query usage reporting enabled, sentio_node=%s", cfg.SentioNodeAddr)
		}
	}

	if err := p.Serve(ctx); err != nil {
		log.Fatalf("proxy stopped: %v", err)
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
