package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	log "sentioxyz/sentio-core/common/log"
)

func main() {
	configPath := flag.String("config", envOrDefault("CK_CONFIG", ""), "path to JSON config file (optional)")
	flag.Parse()

	cfg := loadConfig(*configPath)
	log.Infof("ck_remote_proxy starting. listen=%s upstream=%s dial_timeout=%s idle_timeout=%s stats_interval=%s log_queries=%t log_data=%t", cfg.Listen, cfg.Upstream, cfg.DialTimeout, cfg.IdleTimeout, cfg.StatsInterval, cfg.LogQueries, cfg.LogData)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	proxy := newProxy(cfg, nil)
	if err := proxy.serve(ctx); err != nil {
		log.Fatalf("proxy stopped: %v", err)
	}
}
