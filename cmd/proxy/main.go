package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	log "sentioxyz/sentio-core/common/log"

	proxy "ck_remote_proxy/pkg/proxy"
)

func main() {
	configPath := flag.String("config", envOrDefault("CK_CONFIG", ""), "path to JSON config file (optional)")
	flag.Parse()

	cfg := proxy.LoadConfig(*configPath)
	log.Infof("clickhouse-proxy starting. listen=%s upstream=%s dial_timeout=%s idle_timeout=%s stats_interval=%s log_queries=%t log_data=%t auth_enabled=%t",
		cfg.Listen, cfg.Upstream, cfg.DialTimeout, cfg.IdleTimeout, cfg.StatsInterval, cfg.LogQueries, cfg.LogData, cfg.AuthEnabled)

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

	// Create table rewriter factory
	tableRewriterFactory := proxy.DefaultTableRewriterFactory("sentio")

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

	p := proxy.NewProxy(cfg, validator, rewriter)

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
