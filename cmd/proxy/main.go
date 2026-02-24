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
	log.Infof("clickhouse-proxy starting. listen=%s upstream=%s dial_timeout=%s idle_timeout=%s stats_interval=%s log_queries=%t log_data=%t auth_enabled=%t rewriter_enabled=%t",
		cfg.Listen, cfg.Upstream, cfg.DialTimeout, cfg.IdleTimeout, cfg.StatsInterval, cfg.LogQueries, cfg.LogData, cfg.AuthEnabled, cfg.RewriterEnabled)

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
	if cfg.RewriterEnabled {
		// Load network state
		var networkState proxy.NetworkState
		switch cfg.NetworkStateSource {
		case "file":
			if cfg.NetworkStateFile != "" {
				state, err := proxy.LoadNetworkStateFromYAML(cfg.NetworkStateFile)
				if err != nil {
					log.Fatalf("failed to load network state from file: %v", err)
				}
				networkState = state
			} else {
				log.Warnf("rewriter enabled but no network state file configured, using empty state")
				networkState = proxy.NewInMemoryNetworkState()
			}
		default:
			log.Warnf("unknown network state source %q, using empty state", cfg.NetworkStateSource)
			networkState = proxy.NewInMemoryNetworkState()
		}

		// Create rewriter
		rwConfig := proxy.RewriterConfig{
			Enabled:        true,
			ServiceAddr:    cfg.RewriterServiceAddr,
			LocalIndexerId: cfg.RewriterLocalIndexerId,
			CHUser:         cfg.CHUser,
			CHPassword:     cfg.CHPassword,
			Timeout:        cfg.RewriterTimeout.Duration,
		}
		rw, err := proxy.NewSentioNetworkRewriter(rwConfig, networkState, proxy.DefaultTableRewriterFactory())
		if err != nil {
			log.Warnf("failed to create rewriter: %v, rewriting disabled", err)
		} else {
			rewriter = rw
			defer rw.Close()
			log.Infof("SQL rewriter enabled, service_addr=%s local_indexer_id=%d", cfg.RewriterServiceAddr, cfg.RewriterLocalIndexerId)
		}
	}

	p := proxy.NewProxy(cfg, validator, rewriter)
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
