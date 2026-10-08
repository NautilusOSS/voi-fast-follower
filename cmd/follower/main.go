package main

import (
	"context"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/api"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/config"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/follower"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/health"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/metrics"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/storage"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/voi"
)

func main() {
	configPath := flag.String("config", envOr("CONFIG_PATH", "config.yaml"), "path to config YAML")
	migrationsPath := flag.String("migrations", envOr("MIGRATIONS_PATH", "migrations"), "path to SQL migrations")
	webPath := flag.String("web", envOr("WEB_PATH", "web"), "path to static web assets")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		boot := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
		boot.Error("config error", "err", err)
		os.Exit(1)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: parseLogLevel(cfg.Log.Level)}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	client, err := voi.New(cfg.Node.AlgodURL, cfg.Node.AlgodToken, log)
	if err != nil {
		log.Error("algod client error", "err", err)
		os.Exit(1)
	}

	pgURL := ""
	if cfg.PostgresEnabled() {
		pgURL = cfg.Database.URL
	}
	archPath := ""
	if cfg.ArchiveEnabled() {
		archPath = cfg.Archive.Path
	}
	bundle, err := storage.BuildSinks(ctx, storage.BuildOptions{
		PostgresURL:        pgURL,
		PostgresMigrations: *migrationsPath,
		PostgresInsertMode: cfg.Database.InsertMode,
		PostgresAsync:      cfg.Database.AsyncCommit,
		ArchivePath:        archPath,
		ArchiveSegmentSize: cfg.Archive.SegmentSize,
	})
	if err != nil {
		log.Error("sink setup error", "err", err)
		os.Exit(1)
	}
	defer func() { _ = bundle.Close() }()
	if cfg.Database.AsyncCommit {
		log.Warn("experimental PG async commit enabled; durability is reduced")
	}
	log.Info("sinks ready", "sinks", strings.Join(bundle.Names, "+"))

	m := metrics.Default()
	ht := health.New()
	if bundle.Archive != nil {
		bundle.Archive.OnBytes(func(n int) { m.RecordArchiveBytes(n) })
	}
	if bundle.Multi != nil {
		bundle.Multi.OnCommit(func(name string, d time.Duration, err error) {
			m.ObserveSinkCommit(name, d, err)
		})
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	ht.RegisterHandlers(mux)

	if bundle.Postgres != nil {
		apiServer := &api.Server{DB: bundle.Postgres, Voi: client}
		apiServer.Register(mux)
		mountExplorer(mux, *webPath, log)
	} else {
		log.Info("postgres sink disabled; explorer API unavailable")
	}

	srv := &http.Server{
		Addr:              cfg.Metrics.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("http listening", "addr", cfg.Metrics.Addr, "healthz", "/healthz", "readyz", "/readyz")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server error", "err", err)
		}
	}()

	engine := follower.New(cfg, client, bundle.Primary, m, log).WithHealth(ht)
	err = engine.Run(ctx)

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)

	if err != nil && err != context.Canceled {
		ht.SetFailed(err.Error())
		log.Error("follower stopped with error", "err", err)
		os.Exit(1)
	}
	log.Info("follower stopped cleanly", "health", ht.Snapshot().Mode)
}

func mountExplorer(mux *http.ServeMux, webPath string, log *slog.Logger) {
	index := filepath.Join(webPath, "explorer.html")
	if _, err := os.Stat(index); err != nil {
		log.Warn("explorer html not found; UI disabled", "path", index, "err", err)
		return
	}
	handler := func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, index)
	}
	mux.HandleFunc("/", handler)
	mux.HandleFunc("/explorer", handler)
	mux.HandleFunc("/explorer/", handler)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
