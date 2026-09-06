package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestConfig_Par2RecreateThreshold_Default(t *testing.T) {
	cfg := mergeWithDefault()
	assert.Equal(t, 0.0, cfg.Par2RecreateThreshold)
	assert.Equal(t, 10, cfg.Par2RecreateRedundancy)
}

func TestConfig_Par2RecreateThreshold_FromYaml(t *testing.T) {
	yml := `
par2_recreate_threshold: 0.1
par2_recreate_redundancy: 15
`
	var cfg Config
	require.NoError(t, yaml.Unmarshal([]byte(yml), &cfg))
	cfg = mergeWithDefault(cfg)
	assert.Equal(t, 0.1, cfg.Par2RecreateThreshold)
	assert.Equal(t, 15, cfg.Par2RecreateRedundancy)
}

func TestApplyProviderDefaults(t *testing.T) {
	tests := []struct {
		name string
		in   ProviderConfig
		want ProviderConfig
	}{
		{
			name: "empty provider gets derived defaults",
			in:   ProviderConfig{},
			want: ProviderConfig{
				Connections:           10,
				MinConnections:        5,
				Inflight:              inflightDefault,
				StatInflight:          statInflightDefault,
				IdleTimeout:           2400 * time.Second,
				ReconnectDelaySeconds: 30,
			},
		},
		{
			name: "explicit values are preserved",
			in: ProviderConfig{
				Connections:    40,
				MinConnections: 3,
				Inflight:       2,
				StatInflight:   7,
			},
			want: ProviderConfig{
				Connections:           40,
				MinConnections:        3,
				Inflight:              2,
				StatInflight:          7,
				IdleTimeout:           2400 * time.Second,
				ReconnectDelaySeconds: 30,
			},
		},
		{
			name: "min connections is clamped to connections",
			in:   ProviderConfig{Connections: 4, MinConnections: 99},
			want: ProviderConfig{
				Connections:           4,
				MinConnections:        4,
				Inflight:              inflightDefault,
				StatInflight:          statInflightDefault,
				IdleTimeout:           2400 * time.Second,
				ReconnectDelaySeconds: 30,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := applyProviderDefaults(tt.in)
			if got != tt.want {
				t.Errorf("applyProviderDefaults() = %+v; want %+v", got, tt.want)
			}
		})
	}
}

// TestWorkerBudgetCountsInflight pins the worker budget to the pool's real
// concurrency: each connection carries Inflight requests at once, so counting
// connections alone under-subscribes the pool.
func TestWorkerBudgetCountsInflight(t *testing.T) {
	cfg := mergeWithDefault(Config{
		DownloadProviders: []ProviderConfig{{Connections: 10, Inflight: 4}},
		UploadProviders:   []ProviderConfig{{Connections: 5, Inflight: 2}},
	})

	if got, want := cfg.DownloadWorkers, 40; got != want {
		t.Errorf("DownloadWorkers = %d; want %d", got, want)
	}
	if got, want := cfg.UploadWorkers, 10; got != want {
		t.Errorf("UploadWorkers = %d; want %d", got, want)
	}
	if got, want := cfg.StatConcurrency, statConcurrencyDefault; got != want {
		t.Errorf("StatConcurrency = %d; want %d", got, want)
	}
}

// TestExplicitWorkerCountsWin ensures a user-set budget is not overwritten.
func TestExplicitWorkerCountsWin(t *testing.T) {
	cfg := mergeWithDefault(Config{
		DownloadWorkers:   3,
		UploadWorkers:     7,
		StatConcurrency:   9,
		DownloadProviders: []ProviderConfig{{Connections: 10, Inflight: 4}},
	})

	if cfg.DownloadWorkers != 3 || cfg.UploadWorkers != 7 || cfg.StatConcurrency != 9 {
		t.Errorf("explicit values overwritten: %+v", cfg)
	}
}
