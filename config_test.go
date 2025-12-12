package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestDurationUnmarshal(t *testing.T) {
	var d Duration
	if err := json.Unmarshal([]byte(`"5s"`), &d); err != nil {
		t.Fatalf("unmarshal duration string: %v", err)
	}
	if d.Duration != 5*time.Second {
		t.Fatalf("duration = %s, want 5s", d)
	}

	if err := json.Unmarshal([]byte(`1000000`), &d); err != nil {
		t.Fatalf("unmarshal duration number: %v", err)
	}
	if d.Duration != time.Duration(1000000) {
		t.Fatalf("duration = %d, want 1000000", d.Duration)
	}
}

func TestDefaultConfig(t *testing.T) {
	t.Setenv("CK_LISTEN", "0.0.0.0:19001")
	t.Setenv("CK_UPSTREAM", "127.0.0.1:19000")

	cfg := defaultConfig()
	if cfg.Listen != "0.0.0.0:19001" {
		t.Fatalf("Listen = %s, want env override", cfg.Listen)
	}
	if cfg.Upstream != "127.0.0.1:19000" {
		t.Fatalf("Upstream = %s, want env override", cfg.Upstream)
	}
	if cfg.StatsInterval.Duration == 0 || cfg.DialTimeout.Duration == 0 || cfg.IdleTimeout.Duration == 0 {
		t.Fatalf("expected non-zero defaults, got %+v", cfg)
	}
}
