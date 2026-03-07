package proxy

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

// TestSkipRewriteSettingStripped verifies that stripAuthTokenSettings removes
// SQL_skip_rewrite and skip_rewrite keys from new-format settings.
func TestSkipRewriteSettingStripped(t *testing.T) {
	settings := []proto.Setting{
		{Key: "max_threads", Value: "4"},
		{Key: "SQL_skip_rewrite", Value: "1"},
		{Key: "database", Value: "default"},
		{Key: "skip_rewrite", Value: "1"},
	}
	result := stripAuthTokenSettings(settings)
	if len(result) != 2 {
		t.Fatalf("expected 2 settings after strip, got %d", len(result))
	}
	for _, s := range result {
		if s.Key == "SQL_skip_rewrite" || s.Key == "skip_rewrite" {
			t.Errorf("skip_rewrite key %q should have been stripped", s.Key)
		}
	}
}

// TestSkipRewriteOldSettingStripped verifies that stripAuthTokenOldSettings removes
// SQL_skip_rewrite and skip_rewrite keys from old-format settings.
func TestSkipRewriteOldSettingStripped(t *testing.T) {
	settings := []OldSetting{
		{Key: "max_threads", Value: 4},
		{Key: "SQL_skip_rewrite", Value: 1},
		{Key: "database", Value: 0},
		{Key: "skip_rewrite", Value: 1},
	}
	result := stripAuthTokenOldSettings(settings)
	if len(result) != 2 {
		t.Fatalf("expected 2 settings after strip, got %d", len(result))
	}
	for _, s := range result {
		if s.Key == "SQL_skip_rewrite" || s.Key == "skip_rewrite" {
			t.Errorf("skip_rewrite key %q should have been stripped", s.Key)
		}
	}
}

// TestHasSkipRewriteFlag verifies the hasSkipRewriteFlag helper function.
func TestHasSkipRewriteFlag(t *testing.T) {
	tests := []struct {
		name     string
		settings []proto.Setting
		old      []OldSetting
		want     bool
	}{
		{
			name:     "no settings",
			settings: nil,
			old:      nil,
			want:     false,
		},
		{
			name: "SQL_skip_rewrite=1 in new settings",
			settings: []proto.Setting{
				{Key: "SQL_skip_rewrite", Value: "1"},
			},
			want: true,
		},
		{
			name: "skip_rewrite=1 in new settings",
			settings: []proto.Setting{
				{Key: "skip_rewrite", Value: "1"},
			},
			want: true,
		},
		{
			name: "SQL_skip_rewrite=0 (not enabled)",
			settings: []proto.Setting{
				{Key: "SQL_skip_rewrite", Value: "0"},
			},
			want: false,
		},
		{
			name: "unrelated settings only",
			settings: []proto.Setting{
				{Key: "max_threads", Value: "4"},
			},
			want: false,
		},
		{
			name: "SQL_skip_rewrite=1 in old settings",
			old: []OldSetting{
				{Key: "SQL_skip_rewrite", Value: 1},
			},
			want: true,
		},
		{
			name: "skip_rewrite=0 in old settings (not enabled)",
			old: []OldSetting{
				{Key: "skip_rewrite", Value: 0},
			},
			want: false,
		},
		{
			name: "SQL_skip_rewrite='1' quoted (clickhouse-go CustomSetting)",
			settings: []proto.Setting{
				{Key: "SQL_skip_rewrite", Value: "'1'"},
			},
			want: true,
		},
		{
			name: "SQL_skip_rewrite='0' quoted (not enabled)",
			settings: []proto.Setting{
				{Key: "SQL_skip_rewrite", Value: "'0'"},
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hasSkipRewriteFlag(tt.settings, tt.old)
			if got != tt.want {
				t.Errorf("hasSkipRewriteFlag() = %v, want %v", got, tt.want)
			}
		})
	}
}
