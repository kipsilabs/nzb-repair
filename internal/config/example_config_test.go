package config

import "testing"

// TestExampleConfigParses keeps config.example.yml honest: every documented key
// must still be one the loader understands, and the derived defaults must land
// where the comments say they do.
func TestExampleConfigParses(t *testing.T) {
	cfg, err := NewFromFile("../../config.example.yml")
	if err != nil {
		t.Fatalf("config.example.yml failed to parse: %v", err)
	}

	if len(cfg.DownloadProviders) != 1 {
		t.Fatalf("got %d download providers; want 1", len(cfg.DownloadProviders))
	}
	if len(cfg.UploadProviders) != 1 {
		t.Fatalf("got %d upload providers; want 1", len(cfg.UploadProviders))
	}

	d := cfg.DownloadProviders[0]
	if d.Inflight != 5 {
		t.Errorf("download Inflight = %d; want 5", d.Inflight)
	}
	if d.StatInflight != statInflightDefault {
		t.Errorf("download StatInflight = %d; want %d", d.StatInflight, statInflightDefault)
	}
	if d.MinConnections != 5 {
		t.Errorf("download MinConnections = %d; want 5", d.MinConnections)
	}

	if cfg.StatConcurrency != statConcurrencyDefault {
		t.Errorf("StatConcurrency = %d; want %d", cfg.StatConcurrency, statConcurrencyDefault)
	}

	// 10 connections x 5 inflight, and 5 x 5 for uploads.
	if cfg.DownloadWorkers != 50 {
		t.Errorf("DownloadWorkers = %d; want 50", cfg.DownloadWorkers)
	}
	if cfg.UploadWorkers != 25 {
		t.Errorf("UploadWorkers = %d; want 25", cfg.UploadWorkers)
	}
}
