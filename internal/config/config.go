package config

import (
	"context"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Logger interface compatible with slog.Logger
type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	DebugContext(ctx context.Context, msg string, args ...any)
	InfoContext(ctx context.Context, msg string, args ...any)
	WarnContext(ctx context.Context, msg string, args ...any)
	ErrorContext(ctx context.Context, msg string, args ...any)
}

// ProviderConfig holds YAML-friendly NNTP provider settings that map to nntppool/v4 Provider.
type ProviderConfig struct {
	Host        string        `yaml:"host"`
	Username    string        `yaml:"username"`
	Password    string        `yaml:"password"`
	Port        int           `yaml:"port"`
	Connections int           `yaml:"connections"`
	Inflight    int           `yaml:"inflight"`
	TLS         bool          `yaml:"tls"`
	InsecureSSL bool          `yaml:"insecure_ssl"`
	Backup      bool          `yaml:"backup"`
	IdleTimeout time.Duration `yaml:"idle_timeout"`
	SkipPing    bool          `yaml:"skip_ping"`
	// KeepaliveIntervalSeconds, if > 0, sends a lightweight NNTP command
	// periodically when the connection is idle. Recommended: 30–60.
	KeepaliveIntervalSeconds int `yaml:"keepalive_interval_seconds"`
	// KeepaliveCommand is the NNTP command used as keepalive probe.
	// Defaults to "DATE" when empty. Ignored when KeepaliveIntervalSeconds is 0.
	KeepaliveCommand string `yaml:"keepalive_command"`
	// UserAgent identifies this client to the NNTP server. Empty disables it.
	UserAgent string `yaml:"user_agent"`
	// QuotaBytes is the maximum bytes that may be downloaded from this provider
	// per QuotaPeriodHours. 0 means unlimited.
	QuotaBytes int64 `yaml:"quota_bytes"`
	// QuotaPeriodHours is the rolling window (in hours) after which the quota resets.
	QuotaPeriodHours int `yaml:"quota_period_hours"`
	// ReconnectDelaySeconds is how long (in seconds) to wait before re-adding a
	// provider that was removed after a 502 "service unavailable" response.
	// 0 defaults to 30s; use a negative value to disable auto-reconnect.
	ReconnectDelaySeconds int `yaml:"reconnect_delay_seconds"`
}

type Config struct {
	// By default the number of connections for download providers is the sum of all Connections
	DownloadWorkers   int              `yaml:"download_workers"`
	UploadWorkers     int              `yaml:"upload_workers"`
	DownloadFolder    string           `yaml:"download_folder"`
	DownloadProviders []ProviderConfig `yaml:"download_providers"`
	UploadProviders   []ProviderConfig `yaml:"upload_providers"`
	Par2Exe           string           `yaml:"par2_exe"`
	Upload            UploadConfig     `yaml:"upload"`
	ScanInterval      time.Duration    `yaml:"scan_interval"` // duration string like "5m", "1h"
	MaxRetries        int64            `yaml:"max_retries"`   // maximum number of retries before moving to broken folder
	BrokenFolder      string           `yaml:"broken_folder"` // folder to move broken files to
	// Par2RecreateThreshold is the fraction of missing par2 segments that triggers
	// recreation of the par2 set. 0 = disabled. Example: 0.1 = recreate when ≥10% missing.
	Par2RecreateThreshold float64 `yaml:"par2_recreate_threshold"`
	// Par2RecreateRedundancy is the recovery percentage used when creating a new par2 set.
	Par2RecreateRedundancy int `yaml:"par2_recreate_redundancy"`
	// DownloadRetries is the number of times a single segment download is retried
	// on a transient error (e.g. 502 "too many connections") before giving up.
	DownloadRetries int64 `yaml:"download_retries"`
	// DownloadRetryBaseDelay is the initial backoff between download retries.
	// The delay grows exponentially with jitter, capped at DownloadRetryMaxDelay.
	DownloadRetryBaseDelay time.Duration `yaml:"download_retry_base_delay"`
	// DownloadRetryMaxDelay caps the backoff between download retries.
	DownloadRetryMaxDelay time.Duration `yaml:"download_retry_max_delay"`
}

type UploadConfig struct {
	ObfuscationPolicy ObfuscationPolicy `yaml:"obfuscation_policy"`
}

type ObfuscationPolicy string

const (
	ObfuscationPolicyNone ObfuscationPolicy = "none"
	ObfuscationPolicyFull ObfuscationPolicy = "full"
)

type Option func(*Config)

var (
	providerConfigDefault = ProviderConfig{
		Connections:           10,
		IdleTimeout:           2400 * time.Second,
		ReconnectDelaySeconds: 30,
	}
	downloadWorkersDefault        = 10
	uploadWorkersDefault          = 10
	scanIntervalDefault           = 5 * time.Minute
	maxRetriesDefault             = int64(3)
	brokenFolderDefault           = "broken"
	downloadRetriesDefault        = int64(5)
	downloadRetryBaseDelayDefault = 2 * time.Second
	downloadRetryMaxDelayDefault  = 60 * time.Second
)

func mergeWithDefault(config ...Config) Config {
	if len(config) == 0 {
		return Config{
			DownloadProviders:      []ProviderConfig{},
			UploadProviders:        []ProviderConfig{},
			DownloadWorkers:        downloadWorkersDefault,
			UploadWorkers:          uploadWorkersDefault,
			DownloadFolder:         "./",
			ScanInterval:           scanIntervalDefault,
			MaxRetries:             maxRetriesDefault,
			BrokenFolder:           brokenFolderDefault,
			Par2RecreateRedundancy: 10,
			DownloadRetries:        downloadRetriesDefault,
			DownloadRetryBaseDelay: downloadRetryBaseDelayDefault,
			DownloadRetryMaxDelay:  downloadRetryMaxDelayDefault,
		}
	}

	cfg := config[0]

	downloadWorkers := 0
	for i, p := range cfg.DownloadProviders {
		if p.Connections == 0 {
			p.Connections = providerConfigDefault.Connections
		}

		if p.IdleTimeout == 0 {
			p.IdleTimeout = providerConfigDefault.IdleTimeout
		}

		if p.ReconnectDelaySeconds == 0 {
			p.ReconnectDelaySeconds = providerConfigDefault.ReconnectDelaySeconds
		}

		cfg.DownloadProviders[i] = p
		downloadWorkers += p.Connections
	}

	if cfg.DownloadWorkers == 0 {
		cfg.DownloadWorkers = downloadWorkers
	}

	uploadWorkers := 0
	for i, p := range cfg.UploadProviders {
		if p.Connections == 0 {
			p.Connections = providerConfigDefault.Connections
		}

		if p.IdleTimeout == 0 {
			p.IdleTimeout = providerConfigDefault.IdleTimeout
		}

		if p.ReconnectDelaySeconds == 0 {
			p.ReconnectDelaySeconds = providerConfigDefault.ReconnectDelaySeconds
		}

		cfg.UploadProviders[i] = p
		uploadWorkers += p.Connections
	}

	if cfg.UploadWorkers == 0 {
		cfg.UploadWorkers = uploadWorkers
	}

	if cfg.ScanInterval == 0 {
		cfg.ScanInterval = scanIntervalDefault
	}

	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = maxRetriesDefault
	}

	if cfg.BrokenFolder == "" {
		cfg.BrokenFolder = brokenFolderDefault
	}

	if cfg.Par2RecreateRedundancy == 0 {
		cfg.Par2RecreateRedundancy = 10
	}

	if cfg.DownloadRetries == 0 {
		cfg.DownloadRetries = downloadRetriesDefault
	}

	if cfg.DownloadRetryBaseDelay == 0 {
		cfg.DownloadRetryBaseDelay = downloadRetryBaseDelayDefault
	}

	if cfg.DownloadRetryMaxDelay == 0 {
		cfg.DownloadRetryMaxDelay = downloadRetryMaxDelayDefault
	}

	return cfg
}

func NewFromFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, err
	}

	return mergeWithDefault(cfg), nil
}
