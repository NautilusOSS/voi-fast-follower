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
		"VOI_ALGOD_URL", "VOI_ALGOD_TOKEN", "VOI_START_ROUND", "START_ROUND", "VOI_SYNC_MODE",
		"DATABASE_URL", "PG_INSERT_MODE", "PG_ASYNC_COMMIT",
		"POLL_INTERVAL", "WORKERS", "FETCH_WINDOW",
		"COMMIT_BATCH_SIZE", "COMMIT_FLUSH_INTERVAL",
		"PREFETCH_WORKERS", "PREFETCH_BUFFER",
		"METRICS_ADDR", "VOI_NETWORK", "CONFIG_PATH", "LOG_LEVEL",
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
  workers: 8
  fetch_window: 16
database:
  url: postgres://u:p@localhost/db
metrics:
  addr: ":9191"
log:
  level: debug
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	t.Setenv("VOI_ALGOD_URL", "http://override:8080")
	t.Setenv("VOI_START_ROUND", "latest")
	t.Setenv("POLL_INTERVAL", "50ms")
	t.Setenv("WORKERS", "32")
	t.Setenv("FETCH_WINDOW", "64")
	t.Setenv("COMMIT_BATCH_SIZE", "50")
	t.Setenv("LOG_LEVEL", "warn")

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
	if cfg.Sync.Workers != 32 {
		t.Fatalf("workers=%d", cfg.Sync.Workers)
	}
	// Fetch window is raised to at least CommitBatchSize.
	if cfg.Sync.FetchWindow < 50 {
		t.Fatalf("fetch_window=%d want >= batch size 50", cfg.Sync.FetchWindow)
	}
	if cfg.Sync.CommitBatchSize != 50 {
		t.Fatalf("commit_batch_size=%d", cfg.Sync.CommitBatchSize)
	}
	if cfg.Database.InsertMode != "unnest" {
		t.Fatalf("insert_mode=%q", cfg.Database.InsertMode)
	}
	if cfg.Log.Level != "warn" {
		t.Fatalf("log level=%q", cfg.Log.Level)
	}
	if cfg.Metrics.Addr != ":9191" {
		t.Fatalf("metrics addr = %q", cfg.Metrics.Addr)
	}
}

func TestLegacyPrefetchKeys(t *testing.T) {
	clearConfigEnv(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := `
node:
  algod_url: http://example:8080
sync:
  prefetch_workers: 16
  prefetch_buffer: 48
database:
  url: postgres://u:p@localhost/db
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Sync.Workers != 16 {
		t.Fatalf("workers=%d want 16 from legacy key", cfg.Sync.Workers)
	}
	// Window is raised to at least the default commit batch size (10).
	if cfg.Sync.FetchWindow < 10 {
		t.Fatalf("fetch_window=%d want >= 10", cfg.Sync.FetchWindow)
	}
}

func TestDefaultsWorkers32(t *testing.T) {
	clearConfigEnv(t)
	cfg := defaults()
	cfg.Node.AlgodURL = "http://x"
	cfg.Database.URL = "postgres://x"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Sync.Workers != 32 {
		t.Fatalf("default workers=%d want 32", cfg.Sync.Workers)
	}
	if cfg.Sync.CommitBatchSize != 10 {
		t.Fatalf("default commit_batch_size=%d want 10", cfg.Sync.CommitBatchSize)
	}
	if cfg.Sync.FetchWindow < cfg.Sync.CommitBatchSize {
		t.Fatalf("fetch_window=%d want >= commit_batch_size=%d", cfg.Sync.FetchWindow, cfg.Sync.CommitBatchSize)
	}
	if cfg.Database.InsertMode != "unnest" {
		t.Fatalf("default insert_mode=%q", cfg.Database.InsertMode)
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

func TestPostgresInsertModeEnv(t *testing.T) {
	clearConfigEnv(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(`
node:
  algod_url: http://example:8080
database:
  url: postgres://u:p@localhost/db
`), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.InsertMode != "unnest" {
		t.Fatalf("default insert_mode=%q", cfg.Database.InsertMode)
	}
	t.Setenv("PG_INSERT_MODE", "copy")
	t.Setenv("PG_ASYNC_COMMIT", "true")
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Database.InsertMode != "copy" {
		t.Fatalf("insert_mode=%q", cfg.Database.InsertMode)
	}
	if !cfg.Database.AsyncCommit {
		t.Fatal("expected async_commit")
	}
}
