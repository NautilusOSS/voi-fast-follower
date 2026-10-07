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
	Network string         `yaml:"network"`
	Node    NodeConfig     `yaml:"node"`
	Sync    SyncConfig     `yaml:"sync"`
	Database DatabaseConfig `yaml:"database"`
	Metrics MetricsConfig  `yaml:"metrics"`
}

type NodeConfig struct {
	AlgodURL   string `yaml:"algod_url"`
	AlgodToken string `yaml:"algod_token"`
}

type SyncConfig struct {
	StartRound       string        `yaml:"start_round"`
	Mode             string        `yaml:"mode"`
	PollInterval     time.Duration `yaml:"poll_interval"`
	PrefetchWorkers  int           `yaml:"prefetch_workers"`
	PrefetchBuffer   int           `yaml:"prefetch_buffer"`
}

type DatabaseConfig struct {
	URL string `yaml:"url"`
}

type MetricsConfig struct {
	Addr string `yaml:"addr"`
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
			StartRound:      StartRoundLatest,
			Mode:            "fast",
			PollInterval:    100 * time.Millisecond,
			PrefetchWorkers: 16,
			PrefetchBuffer:  32,
		},
		Database: DatabaseConfig{
			URL: "",
		},
		Metrics: MetricsConfig{
			Addr: ":9090",
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
	if v := os.Getenv("VOI_SYNC_MODE"); v != "" {
		cfg.Sync.Mode = v
	}
	if v := os.Getenv("DATABASE_URL"); v != "" {
		cfg.Database.URL = v
	}
	if v := os.Getenv("POLL_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Sync.PollInterval = d
		}
	}
	if v := os.Getenv("PREFETCH_WORKERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Sync.PrefetchWorkers = n
		}
	}
	if v := os.Getenv("PREFETCH_BUFFER"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Sync.PrefetchBuffer = n
		}
	}
	if v := os.Getenv("METRICS_ADDR"); v != "" {
		cfg.Metrics.Addr = v
	}
	if v := os.Getenv("VOI_NETWORK"); v != "" {
		cfg.Network = v
	}
	if v := os.Getenv("CONFIG_PATH"); v != "" {
		_ = v // documented; Load() receives path from main
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
	if c.Sync.PrefetchWorkers < 1 {
		c.Sync.PrefetchWorkers = 1
	}
	if c.Sync.PrefetchBuffer < 1 {
		c.Sync.PrefetchBuffer = c.Sync.PrefetchWorkers
	}
	if strings.EqualFold(c.Sync.Mode, "fast") {
		// ok
	} else if c.Sync.Mode == "" {
		c.Sync.Mode = "fast"
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
