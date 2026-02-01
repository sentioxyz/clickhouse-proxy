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
)

func main() {
	configPath := flag.String("config", envOrDefault("CK_CONFIG", ""), "path to JSON config file (optional)")
	flag.Parse()

	cfg := loadConfig(*configPath)
	log.Infof("ck_remote_proxy starting. listen=%s upstream=%s dial_timeout=%s idle_timeout=%s stats_interval=%s log_queries=%t log_data=%t auth_enabled=%t",
		cfg.Listen, cfg.Upstream, cfg.DialTimeout, cfg.IdleTimeout, cfg.StatsInterval, cfg.LogQueries, cfg.LogData, cfg.AuthEnabled)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Infof("metrics listening on %s", cfg.MetricsListen)
		if err := http.ListenAndServe(cfg.MetricsListen, promhttp.Handler()); err != nil {
			log.Infof("metrics server error: %v", err)
		}
	}()

	// Create validator based on configuration
	var validator Validator
	if cfg.AuthEnabled {
		validator = NewEthValidator(cfg.AuthAllowedAddresses, cfg.AuthMaxTokenAge.Duration, true, cfg.AuthAllowNoAuth)
		log.Infof("Ethereum signature auth enabled with %d allowed addresses, allow_no_auth=%t", len(cfg.AuthAllowedAddresses), cfg.AuthAllowNoAuth)
	}

	proxy := newProxy(cfg, validator)
	if err := proxy.serve(ctx); err != nil {
		log.Fatalf("proxy stopped: %v", err)
	}
}
