package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const StartRoundLatest = "latest"

// Config holds runtime configuration for the follower.
type Config struct {
	Network  string         `yaml:"network"`
	Node     NodeConfig     `yaml:"node"`
	Sync     SyncConfig     `yaml:"sync"`
	Database DatabaseConfig `yaml:"database"`
	Metrics  MetricsConfig  `yaml:"metrics"`
	Log      LogConfig      `yaml:"log"`
}

type NodeConfig struct {
	AlgodURL   string `yaml:"algod_url"`
	AlgodToken string `yaml:"algod_token"`
}

type SyncConfig struct {
	StartRound   string        `yaml:"start_round"`
	Mode         string        `yaml:"mode"`
	PollInterval time.Duration `yaml:"poll_interval"`
	// Workers is the concurrent BlockRaw fetch pool size (default 32).
	Workers int `yaml:"workers"`
	// FetchWindow bounds in-flight (fetched-but-not-committed) rounds.
	FetchWindow int `yaml:"fetch_window"`
	// CommitBatchSize is the max contiguous blocks per sink commit during
	// catch-up. Live follow forces batch size 1. Default 10.
	CommitBatchSize int `yaml:"commit_batch_size"`
	// CommitFlushInterval flushes a partial catch-up batch after this delay
	// waiting for more contiguous rounds (0 disables time-based flush).
	CommitFlushInterval time.Duration `yaml:"commit_flush_interval"`

	// Legacy YAML keys — still accepted via Unmarshal aliases below.
	PrefetchWorkers int `yaml:"prefetch_workers"`
	PrefetchBuffer  int `yaml:"prefetch_buffer"`
}

type DatabaseConfig struct {
	URL string `yaml:"url"`
	// InsertMode is "unnest" (default) or "copy" for Postgres CommitBatch.
	InsertMode string `yaml:"insert_mode"`
	// AsyncCommit is an experimental opt-in that sets synchronous_commit=off
	// for batch transactions only. Disabled by default.
	AsyncCommit bool `yaml:"async_commit"`
}

type MetricsConfig struct {
	Addr string `yaml:"addr"`
}

type LogConfig struct {
	Level string `yaml:"level"`
}

// Load reads optional YAML from path, then applies environment overrides.
func Load(path string) (*Config, error) {
	cfg := defaults()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, fmt.Errorf("read config: %w", err)
			}
		} else {
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, fmt.Errorf("parse config: %w", err)
			}
		}
	}
	applyEnv(cfg)
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func defaults() *Config {
	return &Config{
		Network: "voi-mainnet",
		Node: NodeConfig{
			AlgodURL: "",
		},
		Sync: SyncConfig{
			StartRound:   StartRoundLatest,
			Mode:         "fast",
			PollInterval: 100 * time.Millisecond,
			// Workers/FetchWindow default in Validate so legacy YAML keys can apply.
			Workers:             0,
			FetchWindow:         0,
			CommitBatchSize:     0, // defaulted in Validate
			CommitFlushInterval: 200 * time.Millisecond,
		},
		Database: DatabaseConfig{
			URL:        "",
			InsertMode: "unnest",
		},
		Metrics: MetricsConfig{
			Addr: ":9090",
		},
		Log: LogConfig{
			Level: "info",
		},
	}
}

func applyEnv(cfg *Config) {
	if v := os.Getenv("VOI_ALGOD_URL"); v != "" {
		cfg.Node.AlgodURL = v
	}
	if v := os.Getenv("VOI_ALGOD_TOKEN"); v != "" {
		cfg.Node.AlgodToken = v
	}
	if v := os.Getenv("VOI_START_ROUND"); v != "" {
		cfg.Sync.StartRound = v
	}
	if v := os.Getenv("START_ROUND"); v != "" {
		cfg.Sync.StartRound = v
	}
	if v := os.Getenv("VOI_SYNC_MODE"); v != "" {
		cfg.Sync.Mode = v
	}
	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.Database.URL = v
	}
	if v := os.Getenv("PG_INSERT_MODE"); v != "" {
		cfg.Database.InsertMode = v
	}
	if v := os.Getenv("PG_ASYNC_COMMIT"); v != "" {
		cfg.Database.AsyncCommit = strings.EqualFold(v, "1") || strings.EqualFold(v, "true") || strings.EqualFold(v, "on")
	}
	if v := os.Getenv("POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Sync.PollInterval = d
		}
	}
	// Legacy aliases first; preferred Phase 1 names win when both are set.
	if v := os.Getenv("PREFETCH_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Sync.Workers = n
		}
	}
	if v := os.Getenv("PREFETCH_BUFFER"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Sync.FetchWindow = n
		}
	}
	if v := os.Getenv("WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Sync.Workers = n
		}
	}
	if v := os.Getenv("FETCH_WINDOW"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Sync.FetchWindow = n
		}
	}
	if v := os.Getenv("COMMIT_BATCH_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Sync.CommitBatchSize = n
		}
	}
	if v := os.Getenv("COMMIT_FLUSH_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Sync.CommitFlushInterval = d
		}
	}
	if v := os.Getenv("METRICS_ADDR"); v != "" {
		cfg.Metrics.Addr = v
	}
	if v := os.Getenv("VOI_NETWORK"); v != "" {
		cfg.Network = v
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.Log.Level = v
	}
}

// Validate checks required fields and normalizes sync settings.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.Node.AlgodURL) == "" {
		return fmt.Errorf("node.algod_url / VOI_ALGOD_URL is required")
	}
	if strings.TrimSpace(c.Database.URL) == "" {
		return fmt.Errorf("database.url / DATABASE_URL is required")
	}
	if c.Sync.PollInterval <= 0 {
		c.Sync.PollInterval = 100 * time.Millisecond
	}

	// Prefer workers/fetch_window; fall back to legacy prefetch_* keys; then defaults.
	if c.Sync.Workers < 1 {
		if c.Sync.PrefetchWorkers > 0 {
			c.Sync.Workers = c.Sync.PrefetchWorkers
		} else {
			c.Sync.Workers = 32
		}
	}
	if c.Sync.FetchWindow < 1 {
		if c.Sync.PrefetchBuffer > 0 {
			c.Sync.FetchWindow = c.Sync.PrefetchBuffer
		} else {
			c.Sync.FetchWindow = c.Sync.Workers * 2
		}
	}
	// Window must be at least large enough for the worker pool to stay busy.
	if c.Sync.FetchWindow < c.Sync.Workers {
		c.Sync.FetchWindow = c.Sync.Workers
	}
	if c.Sync.CommitBatchSize < 1 {
		// Phase 4 local e2e sweet spot (UNNEST); see docs/phase4-postgres-sink.md.
		c.Sync.CommitBatchSize = 10
	}
	// Keep the fetch window large enough to fill a commit batch.
	if c.Sync.FetchWindow < c.Sync.CommitBatchSize {
		c.Sync.FetchWindow = c.Sync.CommitBatchSize
	}
	if c.Sync.CommitFlushInterval < 0 {
		c.Sync.CommitFlushInterval = 0
	}

	if strings.EqualFold(c.Sync.Mode, "fast") {
		// ok
	} else if c.Sync.Mode == "" {
		c.Sync.Mode = "fast"
	}
	if strings.TrimSpace(c.Log.Level) == "" {
		c.Log.Level = "info"
	}
	switch strings.ToLower(strings.TrimSpace(c.Database.InsertMode)) {
	case "", "unnest":
		c.Database.InsertMode = "unnest"
	case "copy":
		c.Database.InsertMode = "copy"
	default:
		return fmt.Errorf("database.insert_mode must be copy or unnest, got %q", c.Database.InsertMode)
	}
	sr := strings.TrimSpace(c.Sync.StartRound)
	if sr == "" {
		c.Sync.StartRound = StartRoundLatest
	} else if !strings.EqualFold(sr, StartRoundLatest) {
		if _, err := strconv.ParseUint(sr, 10, 64); err != nil {
			return fmt.Errorf("sync.start_round must be %q or a uint64: %w", StartRoundLatest, err)
		}
	}
	return nil
}

// IsLatestStart reports whether start_round means network tip.
func (c *Config) IsLatestStart() bool {
	return strings.EqualFold(strings.TrimSpace(c.Sync.StartRound), StartRoundLatest)
}

// ExplicitStartRound returns the configured start round when not "latest".
func (c *Config) ExplicitStartRound() (uint64, bool, error) {
	if c.IsLatestStart() {
		return 0, false, nil
	}
	n, err := strconv.ParseUint(strings.TrimSpace(c.Sync.StartRound), 10, 64)
	if err != nil {
		return 0, false, err
	}
	return n, true, nil
}
