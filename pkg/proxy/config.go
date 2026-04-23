package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	log "sentioxyz/sentio-core/common/log"
	"time"

	"github.com/sentioxyz/clickhouse-proxy/pkg/cluster"
)

// Config controls proxy behavior. All fields have sane defaults so the
// binary can boot without a config file.
type Config struct {
	Listen           string   `json:"listen"             yaml:"listen"`
	Upstream         string   `json:"upstream"           yaml:"upstream"`
	StatsInterval    Duration `json:"stats_interval"     yaml:"stats_interval"`
	DialTimeout      Duration `json:"dial_timeout"       yaml:"dial_timeout"`
	IdleTimeout      Duration `json:"idle_timeout"       yaml:"idle_timeout"`
	LogQueries       bool     `json:"log_queries"        yaml:"log_queries"`
	LogData          bool     `json:"log_data"           yaml:"log_data"`
	MaxQueryLogBytes int      `json:"max_query_log_bytes" yaml:"max_query_log_bytes"`
	MaxDataLogBytes  int      `json:"max_data_log_bytes"  yaml:"max_data_log_bytes"`
	MetricsListen    string   `json:"metrics_listen"     yaml:"metrics_listen"`

	// Authentication configuration
	AuthEnabled          bool     `json:"auth_enabled"           yaml:"auth_enabled"`
	AuthAllowedAddresses []string `json:"auth_allowed_addresses" yaml:"auth_allowed_addresses"`
	AuthMaxTokenAge      Duration `json:"auth_max_token_age"     yaml:"auth_max_token_age"`
	AuthAllowNoAuth      bool     `json:"auth_allow_no_auth"     yaml:"auth_allow_no_auth"` // If true, requests without auth token are allowed

	// RelayPrivateKeyHex is the Ethereum private key used by proxies to sign
	// relay JWS tokens for proxy-to-proxy (__route__) connections.
	// All proxies in the cluster should share the same key.
	// The corresponding address must be in AuthAllowedAddresses.
	RelayPrivateKeyHex string `json:"relay_private_key_hex" yaml:"relay_private_key_hex"`

	// SQL Rewriter configuration
	RewriterServiceAddr string   `json:"rewriter_service_addr" yaml:"rewriter_service_addr"` // sql-rewriter gRPC address (required when enabled)
	RewriterTimeout     Duration `json:"rewriter_timeout"      yaml:"rewriter_timeout"`      // Rewrite timeout

	// ClickHouse manager config path (for sentio-core table mapper, required)
	CkhManagerConfigPath string `json:"ckh_manager_config_path" yaml:"ckh_manager_config_path"`

	// CredentialReplaceEnabled enables automatic credential replacement.
	// When true, the proxy replaces client's user/password with credentials
	// from CkhManagerConfigPath before forwarding to upstream ClickHouse.
	// This allows sidecar clients to connect without knowing the real ClickHouse password.
	CredentialReplaceEnabled bool `json:"credential_replace_enabled" yaml:"credential_replace_enabled"`

	// Network State configuration
	NetworkStateRedis string `json:"network_state_redis" yaml:"network_state_redis"` // Redis address (for statemirror, e.g. "localhost:6379")

	// Streaming bufio size (bytes). Default: 131072 (128KB).
	StreamingBufSize int `json:"streaming_buf_size" yaml:"streaming_buf_size"`

	// ValidateChecksum 是否启用压缩数据的 checksum 校验（CityHash128）
	ValidateChecksum bool `json:"validate_checksum" yaml:"validate_checksum"`

	// MaxConnectionLifetime 单个连接的最大存活时间。
	// 超过此时间后连接将被关闭，防止慢速客户端无限占用资源。
	// 参考 ClickHouse Server 的 TCP 连接管理行为，默认 24h。
	MaxConnectionLifetime Duration `json:"max_connection_lifetime" yaml:"max_connection_lifetime"`

	// R1-16: ShutdownTimeout 优雅关闭时等待在途连接排水的最大时间，默认 30s。
	ShutdownTimeout Duration `json:"shutdown_timeout" yaml:"shutdown_timeout"`

	// Query usage reporting to sentio-node
	SentioNodeAddr    string `json:"sentio_node_addr" yaml:"sentio_node_addr"`       // sentio-node gRPC address for query usage
	QueryUsageEnabled bool   `json:"query_usage_enabled" yaml:"query_usage_enabled"` // Enable query usage reporting

	// Cluster configuration: shard with multiple replicas mode.
	// When Shard is configured, the proxy manages replicas within this shard (health check, routing, pool).
	// When Shard is nil and Upstream is non-empty, backward-compatible single-upstream mode is used.
	Shard       *cluster.ShardConfig       `json:"shard,omitempty" yaml:"shard,omitempty"`
	HealthCheck *cluster.HealthCheckConfig  `json:"health_check,omitempty" yaml:"health_check,omitempty"`
	Pool        *cluster.PoolConfig         `json:"pool,omitempty" yaml:"pool,omitempty"`
	Routing     *cluster.RoutingConfig      `json:"routing,omitempty" yaml:"routing,omitempty"`

	// ForwardingOnly 标记该 proxy 没有绑定 ClickHouse 实例，
	// 所有请求将随机转发给 NetworkState 中的已绑定 proxy。
	// 当 Upstream 为空且 Shard 为 nil 时自动启用，不从 JSON 读取。
	ForwardingOnly bool `json:"-" yaml:"-"`

	// Sidecar mode: proxy sits next to the ClickHouse client, intercepts queries,
	// signs them with JWS token, and forwards to a remote server-side proxy.
	// When enabled, most server-side features (auth validation, SQL rewriting,
	// NetworkState, Redis) are not needed.
	SidecarMode          bool   `json:"sidecar_mode" yaml:"sidecar_mode"`
	SidecarUpstream      string `json:"sidecar_upstream" yaml:"sidecar_upstream"`                  // Remote server-side proxy address (required when sidecar_mode=true)
	SidecarPrivateKeyHex string `json:"sidecar_private_key_hex" yaml:"sidecar_private_key_hex"`    // Sidecar's own Ethereum private key for JWS signing (required when sidecar_mode=true)

	// DatabaseProcessors maps ClickHouse database names to their processor IDs.
	// Used for:
	//   1. SHOW TABLES filtering — only tables prefixed with the processorID are shown.
	//   2. USE rewriting — client can use "USE <processorID>" and the proxy rewrites it
	//      to "USE <database>" transparently.
	// Example:
	//   "sentio_coinbase": "coinbase"
	//   "sentio_ethereum": "ethereum"
	DatabaseProcessors map[string]string `json:"database_processors" yaml:"database_processors"`

	// DefaultProcessorDatabase is the fallback ClickHouse database for processor IDs
	// that are not explicitly listed in DatabaseProcessors. When a client executes
	// "USE <processorID>" and the processorID has no explicit mapping, the proxy
	// rewrites it to "USE <DefaultProcessorDatabase>" and tracks the processorID
	// for that connection so SHOW TABLES still filters correctly.
	DefaultProcessorDatabase string `json:"default_processor_database" yaml:"default_processor_database"`
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
		// R5-4: 数字被解析为纳秒。如果值非常小（< 1秒），可能是运维误用（以为是秒）
		if d.Duration > 0 && d.Duration < time.Second {
			log.Warnf("[config] duration value %d is interpreted as %s (nanoseconds); did you mean %q?",
				n, d.Duration, time.Duration(n)*time.Second)
		}
		return nil
	}
	return fmt.Errorf("duration must be a string (e.g. \"5s\") or number of nanoseconds")
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

// UnmarshalText implements encoding.TextUnmarshaler, used for CLI flag parsing.
func (d *Duration) UnmarshalText(text []byte) error {
	dur, err := time.ParseDuration(string(text))
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", string(text), err)
	}
	d.Duration = dur
	return nil
}

func (d Duration) MarshalYAML() (interface{}, error) {
	return d.Duration.String(), nil
}

func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err == nil {
		dur, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", s, err)
		}
		d.Duration = dur
		return nil
	}
	var n int64
	if err := unmarshal(&n); err == nil {
		d.Duration = time.Duration(n)
		if d.Duration > 0 && d.Duration < time.Second {
			log.Warnf("[config] duration value %d is interpreted as %s (nanoseconds); did you mean %q?",
				n, d.Duration, time.Duration(n)*time.Second)
		}
		return nil
	}
	return fmt.Errorf("duration must be a string (e.g. \"5s\") or number of nanoseconds")
}

func DefaultConfig() Config {
	return Config{
		Listen:           envOrDefault("CK_LISTEN", ":9001"),
		Upstream:         os.Getenv("CK_UPSTREAM"), // empty by default; forwarding-only when unset
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
		// Rewriter defaults
		RewriterServiceAddr: envOrDefault("CK_REWRITER_ADDR", "localhost:50051"),
		RewriterTimeout:     Duration{5 * time.Second},
		// ClickHouse manager config
		CkhManagerConfigPath:    envOrDefault("CKH_MANAGER_CONFIG", ""),
		CredentialReplaceEnabled: true, // enabled by default
		// Network state defaults
		NetworkStateRedis: envOrDefault("CK_NETWORK_STATE_REDIS", ""),
		// Streaming buffer size
		StreamingBufSize:      131072, // 128KB
		ValidateChecksum:      false,
		MaxConnectionLifetime: Duration{24 * time.Hour},
		ShutdownTimeout:       Duration{30 * time.Second},
		// Sidecar defaults (can be driven entirely by env vars, no config file needed)
		SidecarMode:          envOrDefault("CK_SIDECAR", "") == "true",
		SidecarUpstream:      envOrDefault("CK_SIDECAR_UPSTREAM", ""),
		SidecarPrivateKeyHex: envOrDefault("CK_SIDECAR_KEY", ""),
	}
}

func LoadConfig(path string) Config {
	cfg := DefaultConfig()
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
	log.Infof("config loaded from %s: listen=%s upstream=%s",
		path, cfg.Listen, cfg.Upstream)
	return cfg
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
