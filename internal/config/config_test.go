package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"VOI_ALGOD_URL", "VOI_ALGOD_TOKEN", "VOI_START_ROUND", "VOI_SYNC_MODE",
		"DATABASE_URL", "POLL_INTERVAL", "PREFETCH_WORKERS", "PREFETCH_BUFFER",
		"METRICS_ADDR", "VOI_NETWORK", "CONFIG_PATH",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadYAMLAndEnv(t *testing.T) {
	clearConfigEnv(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
network: voi-mainnet
node:
  algod_url: http://example:8080
  algod_token: secret
sync:
  start_round: "100"
  mode: fast
  poll_interval: 250ms
  prefetch_workers: 8
database:
  url: postgres://u:p@localhost/db
metrics:
  addr: ":9191"
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Setenv("VOI_ALGOD_URL", "http://override:8080")
	t.Setenv("VOI_START_ROUND", "latest")
	t.Setenv("POLL_INTERVAL", "50ms")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Node.AlgodURL != "http://override:8080" {
		t.Fatalf("algod url = %q", cfg.Node.AlgodURL)
	}
	if !cfg.IsLatestStart() {
		t.Fatalf("expected latest start, got %q", cfg.Sync.StartRound)
	}
	if cfg.Sync.PollInterval != 50*time.Millisecond {
		t.Fatalf("poll interval = %v", cfg.Sync.PollInterval)
	}
	if cfg.Metrics.Addr != ":9191" {
		t.Fatalf("metrics addr = %q", cfg.Metrics.Addr)
	}
}

func TestValidateRequiresFields(t *testing.T) {
	clearConfigEnv(t)
	_, err := Load("")
	if err == nil {
		t.Fatal("expected error for missing required fields")
	}
}

func TestExplicitStartRound(t *testing.T) {
	clearConfigEnv(t)
	cfg := defaults()
	cfg.Node.AlgodURL = "http://x"
	cfg.Database.URL = "postgres://x"
	cfg.Sync.StartRound = "25000000"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	n, ok, err := cfg.ExplicitStartRound()
	if err != nil || !ok || n != 25000000 {
		t.Fatalf("got %d ok=%v err=%v", n, ok, err)
	}
}
