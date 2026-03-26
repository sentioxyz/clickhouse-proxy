package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap/zapcore"

	ckhmanager "sentioxyz/sentio-core/common/clickhousemanager"
	log "sentioxyz/sentio-core/common/log"
	"sentioxyz/sentio-core/network/sqlrewriter"

	proxy "ck_remote_proxy/pkg/proxy"
)

func main() {
	configPath := flag.String("config", envOrDefault("CK_CONFIG", ""), "path to JSON config file (optional)")
	flag.Parse()

	cfg := proxy.LoadConfig(*configPath)

	// Apply log level from config (default: info)
	switch cfg.LogLevel {
	case "debug":
		log.ManuallySetLevel(zapcore.DebugLevel)
	case "info":
		log.ManuallySetLevel(zapcore.InfoLevel)
	case "warn", "warning":
		log.ManuallySetLevel(zapcore.WarnLevel)
	case "error":
		log.ManuallySetLevel(zapcore.ErrorLevel)
	default:
		log.Warnf("unknown log_level %q, defaulting to info", cfg.LogLevel)
		log.ManuallySetLevel(zapcore.InfoLevel)
	}

	log.Infof("clickhouse-proxy starting. listen=%s upstream=%s dial_timeout=%s idle_timeout=%s stats_interval=%s log_queries=%t log_data=%t auth_enabled=%t log_level=%s",
		cfg.Listen, cfg.Upstream, cfg.DialTimeout, cfg.IdleTimeout, cfg.StatsInterval, cfg.LogQueries, cfg.LogData, cfg.AuthEnabled, cfg.LogLevel)

	// Detect forwarding-only mode: no local ClickHouse instance bound
	if cfg.Upstream == "" {
		cfg.ForwardingOnly = true
		log.Infof("forwarding-only mode: no upstream configured, requests will be forwarded to bound proxies via NetworkState")
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

	// Create validator based on configuration
	var validator proxy.Validator
	if cfg.AuthEnabled {
		validator = proxy.NewEthValidator(cfg.AuthAllowedAddresses, cfg.AuthMaxTokenAge.Duration, true, cfg.AuthAllowNoAuth)
		log.Infof("Ethereum signature auth enabled with %d allowed addresses, allow_no_auth=%t", len(cfg.AuthAllowedAddresses), cfg.AuthAllowNoAuth)
	}

	// Create rewriter based on configuration
	var rewriter proxy.Rewriter
	// Load network state
	var networkState proxy.NetworkState
	if cfg.NetworkStateRedis != "" {
		state, err := proxy.NewRedisNetworkState(cfg.NetworkStateRedis)
		if err != nil {
			log.Fatalf("failed to connect to Redis network state: %v", err)
		}
		defer state.Close()
		networkState = state
	} else {
		log.Fatalf("network_state_redis is required for network state")
	}

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

	p := proxy.NewProxy(cfg, validator, rewriter)
	p.SetNetworkState(networkState)

	// Initialize relay JWS signer for proxy-to-proxy (__route__) token propagation
	if cfg.RelayPrivateKeyHex != "" {
		signer, err := proxy.NewRelaySigner(cfg.RelayPrivateKeyHex)
		if err != nil {
			log.Fatalf("failed to create relay signer: %v", err)
		}
		p.SetRelaySigner(signer)
		log.Infof("relay JWS signer enabled, address=%s", signer.Address())
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
