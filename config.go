package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	log "sentioxyz/sentio-core/common/log"
	"time"
)

// Config controls proxy behavior. All fields have sane defaults so the
// binary can boot without a config file.
type Config struct {
	Listen           string   `json:"listen"`
	Upstream         string   `json:"upstream"`
	StatsInterval    Duration `json:"stats_interval"`
	DialTimeout      Duration `json:"dial_timeout"`
	IdleTimeout      Duration `json:"idle_timeout"`
	LogQueries       bool     `json:"log_queries"`
	LogData          bool     `json:"log_data"`
	MaxQueryLogBytes int      `json:"max_query_log_bytes"`
	MaxDataLogBytes  int      `json:"max_data_log_bytes"`
	MetricsListen    string   `json:"metrics_listen"`

	// Authentication configuration
	AuthEnabled          bool     `json:"auth_enabled"`
	AuthAllowedAddresses []string `json:"auth_allowed_addresses"`
	AuthMaxTokenAge      Duration `json:"auth_max_token_age"`
	AuthAllowNoAuth      bool     `json:"auth_allow_no_auth"` // If true, requests without auth token are allowed

	// SQL Rewriter configuration
	RewriterEnabled        bool     `json:"rewriter_enabled"`          // 是否启用 SQL 重写
	RewriterServiceAddr    string   `json:"rewriter_service_addr"`     // sql-rewriter gRPC 服务地址
	RewriterLocalIndexerId uint64   `json:"rewriter_local_indexer_id"` // 本地 Indexer ID
	RewriterTimeout        Duration `json:"rewriter_timeout"`          // 重写超时时间

	// Network State configuration
	NetworkStateSource   string `json:"network_state_source"`   // 状态源: "file" 或 "postgres"
	NetworkStateFile     string `json:"network_state_file"`     // 状态文件路径
	NetworkStatePostgres string `json:"network_state_postgres"` // PostgreSQL 连接串

	// ClickHouse credentials for remote table access
	CHUser     string `json:"ch_user"`     // ClickHouse 用户名
	CHPassword string `json:"ch_password"` // ClickHouse 密码

	// Streaming bufio size (bytes). Default: 131072 (128KB).
	StreamingBufSize int `json:"streaming_buf_size"`

	// ValidateChecksum 是否启用压缩数据的 checksum 校验（CityHash128）
	ValidateChecksum bool `json:"validate_checksum"`
}

// Duration wraps time.Duration to allow human-friendly strings in JSON
// configs (e.g. "5s").
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		dur, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		d.Duration = dur
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err == nil {
		d.Duration = time.Duration(n)
		return nil
	}
	return fmt.Errorf("duration must be a string (e.g. \"5s\") or number of nanoseconds")
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

func defaultConfig() Config {
	return Config{
		Listen:           envOrDefault("CK_LISTEN", ":9001"),
		Upstream:         envOrDefault("CK_UPSTREAM", "clickhouse:9000"),
		StatsInterval:    Duration{10 * time.Second},
		DialTimeout:      Duration{5 * time.Second},
		IdleTimeout:      Duration{5 * time.Minute},
		LogQueries:       true,
		LogData:          false,
		MaxQueryLogBytes: 300,
		MaxDataLogBytes:  200,
		MetricsListen:    envOrDefault("CK_METRICS_LISTEN", ":9091"),
		// Auth defaults: disabled by default
		AuthEnabled:          false,
		AuthAllowedAddresses: nil,
		AuthMaxTokenAge:      Duration{1 * time.Minute},
		AuthAllowNoAuth:      false,
		// Rewriter defaults: disabled by default
		RewriterEnabled:        false,
		RewriterServiceAddr:    envOrDefault("CK_REWRITER_ADDR", "localhost:50051"),
		RewriterLocalIndexerId: 0,
		RewriterTimeout:        Duration{5 * time.Second},
		// Network state defaults
		NetworkStateSource:   envOrDefault("CK_NETWORK_STATE_SOURCE", "file"),
		NetworkStateFile:     envOrDefault("CK_NETWORK_STATE_FILE", ""),
		NetworkStatePostgres: envOrDefault("CK_NETWORK_STATE_POSTGRES", ""),
		// ClickHouse credentials
		CHUser:     envOrDefault("CK_CH_USER", "default"),
		CHPassword: envOrDefault("CK_CH_PASSWORD", ""),
		// Streaming buffer size
		StreamingBufSize: 131072, // 128KB
		ValidateChecksum: false,
	}
}

func loadConfig(path string) Config {
	cfg := defaultConfig()
	if path == "" {
		if _, err := os.Stat("config.json"); err == nil {
			path = "config.json"
		} else {
			log.Infof("no config file provided, using defaults and env overrides")
			return cfg
		}
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			log.Infof("config file %s not found, using defaults", path)
			return cfg
		}
		log.Fatalf("read config file %s: %v", path, err)
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		log.Fatalf("parse config file %s: %v", path, err)
	}
	return cfg
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
