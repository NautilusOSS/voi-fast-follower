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

	sink, err := storage.NewPostgres(ctx, cfg.Database.URL, *migrationsPath)
	if err != nil {
		log.Error("postgres error", "err", err)
		os.Exit(1)
	}
	defer sink.Close()

	m := metrics.Default()
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	apiServer := &api.Server{DB: sink, Voi: client}
	apiServer.Register(mux)
	mountExplorer(mux, *webPath, log)

	srv := &http.Server{
		Addr:              cfg.Metrics.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Info("http listening", "addr", cfg.Metrics.Addr, "explorer", "/")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http server error", "err", err)
		}
	}()

	engine := follower.New(cfg, client, sink, m, log)
	err = engine.Run(ctx)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)

	if err != nil && err != context.Canceled {
		log.Error("follower stopped with error", "err", err)
		os.Exit(1)
	}
	log.Info("follower stopped")
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
