// Command bench measures Voi Fast Follower throughput.
//
// Modes:
//
//	fetch-only   – concurrent BlockRaw + decode, no persistence
//	e2e          – full ingestion into Postgres (ordered commit + checkpoint)
//	sweep        – e2e across COMMIT_BATCH_SIZE values
//	sink-compare – UNNEST vs COPY across batch sizes (Phase 4)
//	workers      – fetch-only across worker counts (8/16/32/64)
//
// Example:
//
//	go run ./cmd/bench -mode fetch-only -count 1000 -workers 32
//	go run ./cmd/bench -mode e2e -count 1000 -workers 32 -batch 50 -reset
//	go run ./cmd/bench -mode sink-compare -count 5000 -workers 32
//	go run ./cmd/bench -mode workers -count 1000
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/config"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/follower"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/metrics"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/storage"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/voi"
)

func main() {
	mode := flag.String("mode", "fetch-only", "fetch-only | e2e | sweep | sink-compare | pipelines | workers")
	start := flag.Uint64("start", 0, "start round (0 = tip-count)")
	count := flag.Uint64("count", 1000, "number of rounds to process")
	workers := flag.Int("workers", 32, "concurrent fetch workers")
	window := flag.Int("window", 0, "fetch window (0 = max(workers*2, batch))")
	batch := flag.Int("batch", 10, "commit batch size (e2e)")
	flush := flag.Duration("flush", 200*time.Millisecond, "commit flush interval")
	insertMode := flag.String("insert-mode", envOr("PG_INSERT_MODE", "unnest"), "postgres insert mode: unnest | copy")
	asyncCommit := flag.Bool("async-commit", false, "experimental: SET LOCAL synchronous_commit=off")
	pipeline := flag.String("pipeline", "postgres", "e2e pipeline: postgres | archive | both")
	archivePath := flag.String("archive", envOr("ARCHIVE_PATH", ""), "archive root (archive/both pipelines)")
	algodURL := flag.String("algod", envOr("VOI_ALGOD_URL", "https://mainnet-api.voi.nodely.dev"), "algod URL")
	token := flag.String("token", envOr("VOI_ALGOD_TOKEN", ""), "algod token")
	dsn := flag.String("database", envOr("DATABASE_URL", "postgres://follower:follower@localhost:5432/voi_follower?sslmode=disable"), "postgres DSN (e2e)")
	migrations := flag.String("migrations", "migrations", "migrations path (e2e)")
	reset := flag.Bool("reset", false, "reset DB rows in range before e2e run")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	ctx := context.Background()

	client, err := voi.New(*algodURL, *token, log)
	if err != nil {
		fmt.Fprintf(os.Stderr, "client: %v\n", err)
		os.Exit(1)
	}

	tip, err := client.LastRound(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "tip: %v\n", err)
		os.Exit(1)
	}

	startRound := *start
	if startRound == 0 {
		if tip < *count {
			startRound = 1
		} else {
			startRound = tip - *count + 1
		}
	}
	endRound := startRound + *count - 1
	if endRound > tip {
		endRound = tip
	}
	rounds := endRound - startRound + 1

	fmt.Printf("=== Voi Fast Follower bench (%s) ===\n", *mode)
	fmt.Printf("algod:     %s\n", *algodURL)
	fmt.Printf("start:     %d\n", startRound)
	fmt.Printf("end:       %d\n", endRound)
	fmt.Printf("rounds:    %d\n", rounds)
	fmt.Printf("workers:   %d\n", *workers)
	fmt.Printf("insert:    %s\n", *insertMode)
	fmt.Printf("pipeline:  %s\n", *pipeline)
	fmt.Printf("async_cmt: %v\n", *asyncCommit)
	fmt.Printf("tip:       %d\n", tip)
	fmt.Printf("go:        %s/%s\n\n", runtime.GOOS, runtime.GOARCH)

	switch *mode {
	case "fetch-only":
		elapsed, stats, errs, err := benchFetchOnly(ctx, client, startRound, endRound, *workers)
		if err != nil {
			fmt.Fprintf(os.Stderr, "fetch-only failed: %v\n", err)
			os.Exit(1)
		}
		printFetchResult(startRound, endRound, rounds, *workers, elapsed, stats, errs)
	case "e2e":
		win := *window
		if win < 1 {
			win = *workers * 2
			if win < *batch {
				win = *batch
			}
		}
		arch := *archivePath
		if arch == "" && (*pipeline == "archive" || *pipeline == "both") {
			arch = os.TempDir() + "/voi-ff-bench-archive"
		}
		elapsed, stats, phase, err := benchE2E(ctx, client, log, e2eOpts{
			dsn: *dsn, migrations: *migrations, archive: arch, pipeline: *pipeline,
			from: startRound, to: endRound, workers: *workers, window: win, batch: *batch,
			flush: *flush, insertMode: *insertMode, asyncCommit: *asyncCommit, reset: *reset,
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "e2e failed: %v\n", err)
			os.Exit(1)
		}
		printE2EResult(startRound, endRound, rounds, *batch, *insertMode, *pipeline, elapsed, stats, phase)
	case "pipelines":
		win := *window
		if win < 1 {
			win = *workers * 2
			if win < *batch {
				win = *batch
			}
		}
		fmt.Println("| Pipeline | Blocks | Elapsed | Blocks/sec | Avg Commit | p50 | p95 | Notes |")
		fmt.Println("|---|---:|---:|---:|---:|---:|---:|---|")
		{
			elapsed, stats, errs, err := benchFetchOnly(ctx, client, startRound, endRound, *workers)
			if err != nil {
				fmt.Fprintf(os.Stderr, "fetch-only: %v\n", err)
				os.Exit(1)
			}
			bps := float64(rounds) / elapsed.Seconds()
			avg, p50, p95 := stats.summary()
			fmt.Printf("| Fetch only | %d | %s | %.2f | %s | %s | %s | errs=%d |\n",
				rounds, elapsed.Round(time.Millisecond), bps,
				avg.Round(time.Microsecond), p50.Round(time.Microsecond), p95.Round(time.Microsecond), errs)
		}
		for _, pipe := range []string{"postgres", "archive", "both"} {
			arch := *archivePath
			if arch == "" {
				arch, _ = os.MkdirTemp("", "voi-ff-arch-"+pipe+"-*")
			} else {
				arch = filepath.Join(arch, pipe)
				_ = os.RemoveAll(arch)
			}
			if pipe == "postgres" || pipe == "both" {
				_ = truncateFollowerDB(ctx, *dsn, *migrations)
			}
			elapsed, stats, _, err := benchE2E(ctx, client, log, e2eOpts{
				dsn: *dsn, migrations: *migrations, archive: arch, pipeline: pipe,
				from: startRound, to: endRound, workers: *workers, window: win, batch: *batch,
				flush: *flush, insertMode: *insertMode, asyncCommit: *asyncCommit, reset: true,
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "pipeline %s: %v\n", pipe, err)
				os.Exit(1)
			}
			bps := float64(rounds) / elapsed.Seconds()
			avg, p50, p95 := stats.summary()
			fmt.Printf("| %s | %d | %s | %.2f | %s | %s | %s | batch=%d |\n",
				pipe, rounds, elapsed.Round(time.Millisecond), bps,
				avg.Round(time.Microsecond), p50.Round(time.Microsecond), p95.Round(time.Microsecond), *batch)
		}
	case "sweep":
		batches := []int{10, 50, 100, 250, 500}
		fmt.Println("| Endpoint | Mode | Batch | Blocks | Elapsed | Blocks/sec | Avg Commit | p50 | p95 |")
		fmt.Println("|---|---|---:|---:|---:|---:|---:|---:|---:|")
		for _, b := range batches {
			if err := truncateFollowerDB(ctx, *dsn, *migrations); err != nil {
				fmt.Fprintf(os.Stderr, "truncate: %v\n", err)
				os.Exit(1)
			}
			win := *window
			if win < 1 {
				win = *workers * 2
				if win < b {
					win = b
				}
			}
			elapsed, stats, _, err := benchE2E(ctx, client, log, e2eOpts{
				dsn: *dsn, migrations: *migrations, pipeline: "postgres",
				from: startRound, to: endRound, workers: *workers, window: win, batch: b,
				flush: *flush, insertMode: *insertMode, asyncCommit: *asyncCommit, reset: true,
			})
			if err != nil {
				fmt.Fprintf(os.Stderr, "sweep failed batch=%d: %v\n", b, err)
				os.Exit(1)
			}
			bps := float64(rounds) / elapsed.Seconds()
			avg, p50, p95 := stats.summary()
			fmt.Printf("| %s | %s | %d | %d | %s | %.2f | %s | %s | %s |\n",
				shortEndpoint(*algodURL), strings.ToUpper(*insertMode), b, rounds, elapsed.Round(time.Millisecond), bps,
				avg.Round(time.Microsecond), p50.Round(time.Microsecond), p95.Round(time.Microsecond))
		}
	case "sink-compare":
		modes := []string{storage.InsertModeUNNEST, storage.InsertModeCOPY}
		batches := []int{10, 50, 100, 250, 500}
		// Disable time-based partial flush so batch size is the controlled variable.
		compareFlush := *flush
		if compareFlush < time.Minute {
			compareFlush = time.Hour
		}
		fmt.Printf("flush:     %s (full batches for fair compare)\n\n", compareFlush)
		fmt.Println("| Implementation | Batch | Blocks | Elapsed | Blocks/sec | Avg Commit | p50 | p95 | Write avg | Commit-phase avg |")
		fmt.Println("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
		for _, im := range modes {
			for _, b := range batches {
				if err := truncateFollowerDB(ctx, *dsn, *migrations); err != nil {
					fmt.Fprintf(os.Stderr, "truncate: %v\n", err)
					os.Exit(1)
				}
				win := *window
				if win < 1 {
					win = *workers * 2
					if win < b {
						win = b
					}
				}
				var ms runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&ms)
				heapBefore := ms.Alloc

				elapsed, stats, phase, err := benchE2E(ctx, client, log, e2eOpts{
					dsn: *dsn, migrations: *migrations, pipeline: "postgres",
					from: startRound, to: endRound, workers: *workers, window: win, batch: b,
					flush: compareFlush, insertMode: im, asyncCommit: *asyncCommit, reset: true,
				})
				if err != nil {
					fmt.Fprintf(os.Stderr, "sink-compare failed mode=%s batch=%d: %v\n", im, b, err)
					os.Exit(1)
				}
				runtime.ReadMemStats(&ms)
				bps := float64(rounds) / elapsed.Seconds()
				avg, p50, p95 := stats.summary()
				fmt.Printf("| %s | %d | %d | %s | %.2f | %s | %s | %s | %s | %s |\n",
					strings.ToUpper(im), b, rounds, elapsed.Round(time.Millisecond), bps,
					avg.Round(time.Microsecond), p50.Round(time.Microsecond), p95.Round(time.Microsecond),
					phase.writeAvg.Round(time.Microsecond), phase.commitAvg.Round(time.Microsecond))
				_ = heapBefore
				fmt.Fprintf(os.Stderr, "# heap_delta_%s_b%d ≈ %d KiB\n", im, b, int64(ms.Alloc-heapBefore)/1024)
			}
		}
	case "workers":
		counts := []int{8, 16, 32, 64}
		fmt.Println("| Endpoint | Workers | Blocks | Elapsed | Blocks/sec | p50 | p95 | Errors |")
		fmt.Println("|---|---:|---:|---:|---:|---:|---:|---:|")
		for _, w := range counts {
			elapsed, stats, errs, err := benchFetchOnly(ctx, client, startRound, endRound, w)
			if err != nil {
				fmt.Fprintf(os.Stderr, "workers failed w=%d: %v\n", w, err)
				os.Exit(1)
			}
			bps := float64(rounds) / elapsed.Seconds()
			_, p50, p95 := stats.summary()
			fmt.Printf("| %s | %d | %d | %s | %.2f | %s | %s | %d |\n",
				shortEndpoint(*algodURL), w, rounds, elapsed.Round(time.Millisecond), bps,
				p50.Round(time.Microsecond), p95.Round(time.Microsecond), errs)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n", *mode)
		os.Exit(2)
	}
}

func benchFetchOnly(ctx context.Context, client *voi.Client, from, to uint64, workers int) (time.Duration, *latencyStats, uint64, error) {
	n := int(to - from + 1)
	stats := &latencyStats{}
	var okCount, errCount atomic.Uint64
	start := time.Now()
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(workers)
	for i := 0; i < n; i++ {
		round := from + uint64(i)
		g.Go(func() error {
			t0 := time.Now()
			blk, err := client.GetBlock(gctx, round)
			stats.observe(time.Since(t0))
			if err != nil {
				errCount.Add(1)
				return err
			}
			if blk.Round != round {
				errCount.Add(1)
				return fmt.Errorf("round mismatch want %d got %d", round, blk.Round)
			}
			okCount.Add(1)
			return nil
		})
	}
	err := g.Wait()
	elapsed := time.Since(start)
	if err != nil {
		return elapsed, stats, errCount.Load(), err
	}
	if okCount.Load() != uint64(n) {
		return elapsed, stats, errCount.Load(), fmt.Errorf("processed %d want %d", okCount.Load(), n)
	}
	return elapsed, stats, errCount.Load(), nil
}

type latencyStats struct {
	mu   sync.Mutex
	durs []time.Duration
}

func (s *latencyStats) observe(d time.Duration) {
	s.mu.Lock()
	s.durs = append(s.durs, d)
	s.mu.Unlock()
}

func (s *latencyStats) summary() (avg, p50, p95 time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.durs) == 0 {
		return 0, 0, 0
	}
	sorted := append([]time.Duration(nil), s.durs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	avg = sum / time.Duration(len(sorted))
	p50 = sorted[len(sorted)*50/100]
	idx := len(sorted) * 95 / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	p95 = sorted[idx]
	return avg, p50, p95
}

type timingSink struct {
	inner storage.BatchBlockSink
	stats *latencyStats
}

func (t *timingSink) Commit(ctx context.Context, blk block.Block) error {
	return t.CommitBatch(ctx, []block.Block{blk})
}
func (t *timingSink) CommitBatch(ctx context.Context, blocks []block.Block) error {
	start := time.Now()
	err := t.inner.CommitBatch(ctx, blocks)
	t.stats.observe(time.Since(start))
	return err
}
func (t *timingSink) LastProcessedRound(ctx context.Context) (uint64, bool, error) {
	return t.inner.LastProcessedRound(ctx)
}
func (t *timingSink) MaxBlockRound(ctx context.Context) (uint64, bool, error) {
	return t.inner.MaxBlockRound(ctx)
}
func (t *timingSink) BlockHash(ctx context.Context, round uint64) (string, bool, error) {
	return t.inner.BlockHash(ctx, round)
}
func (t *timingSink) Close() error { return t.inner.Close() }

type phaseAgg struct {
	writeAvg  time.Duration
	commitAvg time.Duration
	n         int
}

type e2eOpts struct {
	dsn, migrations, archive, pipeline string
	from, to                           uint64
	workers, window, batch             int
	flush                              time.Duration
	insertMode                         string
	asyncCommit, reset                 bool
}

func benchE2E(ctx context.Context, client *voi.Client, log *slog.Logger, o e2eOpts) (time.Duration, *latencyStats, phaseAgg, error) {
	var phase phaseAgg
	pgURL, archPath := "", ""
	switch strings.ToLower(o.pipeline) {
	case "", "postgres":
		pgURL = o.dsn
	case "archive":
		archPath = o.archive
		if archPath == "" {
			return 0, nil, phase, fmt.Errorf("archive pipeline requires -archive path")
		}
		_ = os.RemoveAll(archPath)
	case "both":
		pgURL = o.dsn
		archPath = o.archive
		if archPath == "" {
			return 0, nil, phase, fmt.Errorf("both pipeline requires -archive path")
		}
		_ = os.RemoveAll(archPath)
	default:
		return 0, nil, phase, fmt.Errorf("unknown pipeline %q", o.pipeline)
	}

	bundle, err := storage.BuildSinks(ctx, storage.BuildOptions{
		PostgresURL:        pgURL,
		PostgresMigrations: o.migrations,
		PostgresInsertMode: o.insertMode,
		PostgresAsync:      o.asyncCommit,
		ArchivePath:        archPath,
	})
	if err != nil {
		return 0, nil, phase, err
	}
	defer bundle.Close()

	var writeSum, commitSum time.Duration
	var phaseN int
	if bundle.Postgres != nil {
		bundle.Postgres.OnTimings(func(t storage.CommitPhaseTimings) {
			writeSum += t.Writes
			commitSum += t.Commit
			phaseN++
		})
	}

	if o.reset && bundle.Postgres != nil {
		sink := bundle.Postgres
		if _, err := sink.Pool().Exec(ctx, `DELETE FROM transactions WHERE round >= $1 AND round <= $2`, o.from, o.to); err != nil {
			return 0, nil, phase, err
		}
		if _, err := sink.Pool().Exec(ctx, `DELETE FROM blocks WHERE round >= $1 AND round <= $2`, o.from, o.to); err != nil {
			return 0, nil, phase, err
		}
		if _, err := sink.Pool().Exec(ctx, `
			INSERT INTO sync_state(key, value) VALUES ('last_processed_round', $1)
			ON CONFLICT (key) DO UPDATE SET value = $1
		`, int64(o.from-1)); err != nil {
			return 0, nil, phase, err
		}
	}

	stats := &latencyStats{}
	timed := &timingSink{inner: bundle.Primary, stats: stats}

	cfg := &config.Config{
		Network: "voi-mainnet",
		Sync: config.SyncConfig{
			StartRound:          fmt.Sprintf("%d", o.from),
			Mode:                "fast",
			PollInterval:        50 * time.Millisecond,
			Workers:             o.workers,
			FetchWindow:         o.window,
			CommitBatchSize:     o.batch,
			CommitFlushInterval: o.flush,
		},
		Database: config.DatabaseConfig{URL: pgURL, InsertMode: o.insertMode},
		Archive:  config.ArchiveConfig{Enabled: archPath != "", Path: archPath},
	}

	m := metrics.New()
	engine := follower.New(cfg, client, timed, m, log).WithUntilRound(o.to)

	start := time.Now()
	err = engine.Run(ctx)
	elapsed := time.Since(start)
	if phaseN > 0 {
		phase.writeAvg = writeSum / time.Duration(phaseN)
		phase.commitAvg = commitSum / time.Duration(phaseN)
		phase.n = phaseN
	}
	if err != nil {
		return elapsed, stats, phase, err
	}
	last, ok, err := bundle.Primary.LastProcessedRound(ctx)
	if err != nil {
		return elapsed, stats, phase, err
	}
	if !ok || last < o.to {
		return elapsed, stats, phase, fmt.Errorf("checkpoint=%d ok=%v want >= %d", last, ok, o.to)
	}
	return elapsed, stats, phase, nil
}

func printFetchResult(start, end, rounds uint64, workers int, elapsed time.Duration, stats *latencyStats, errs uint64) {
	bps := float64(rounds) / elapsed.Seconds()
	avg, p50, p95 := stats.summary()
	fmt.Println("=== Results ===")
	fmt.Printf("start_round:     %d\n", start)
	fmt.Printf("end_round:       %d\n", end)
	fmt.Printf("rounds:          %d\n", rounds)
	fmt.Printf("workers:         %d\n", workers)
	fmt.Printf("elapsed:         %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("blocks_per_sec:  %.2f\n", bps)
	fmt.Printf("fetch_avg:       %s\n", avg.Round(time.Microsecond))
	fmt.Printf("fetch_p50:       %s\n", p50.Round(time.Microsecond))
	fmt.Printf("fetch_p95:       %s\n", p95.Round(time.Microsecond))
	fmt.Printf("errors:          %d\n", errs)
}

func printE2EResult(start, end, rounds uint64, batch int, insertMode, pipeline string, elapsed time.Duration, stats *latencyStats, phase phaseAgg) {
	bps := float64(rounds) / elapsed.Seconds()
	avg, p50, p95 := stats.summary()
	fmt.Println("=== Results ===")
	fmt.Printf("start_round:     %d\n", start)
	fmt.Printf("end_round:       %d\n", end)
	fmt.Printf("rounds:          %d\n", rounds)
	fmt.Printf("batch_size:      %d\n", batch)
	fmt.Printf("pipeline:        %s\n", pipeline)
	fmt.Printf("insert_mode:     %s\n", insertMode)
	fmt.Printf("elapsed:         %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("blocks_per_sec:  %.2f\n", bps)
	fmt.Printf("commit_avg:      %s\n", avg.Round(time.Microsecond))
	fmt.Printf("commit_p50:      %s\n", p50.Round(time.Microsecond))
	fmt.Printf("commit_p95:      %s\n", p95.Round(time.Microsecond))
	fmt.Printf("commit_samples:  %d\n", len(stats.durs))
	if phase.n > 0 {
		fmt.Printf("write_avg:       %s\n", phase.writeAvg.Round(time.Microsecond))
		fmt.Printf("pg_commit_avg:   %s\n", phase.commitAvg.Round(time.Microsecond))
	}
}

func truncateFollowerDB(ctx context.Context, dsn, migrations string) error {
	sink, err := storage.NewPostgres(ctx, dsn, migrations)
	if err != nil {
		return err
	}
	defer sink.Close()
	_, err = sink.Pool().Exec(ctx, `TRUNCATE transactions, blocks, sync_state`)
	return err
}

func shortEndpoint(url string) string {
	switch {
	case strings.Contains(url, "127.0.0.1") || strings.Contains(url, "localhost") || strings.Contains(url, ":4001"):
		return "Local"
	case strings.Contains(url, "nodely"):
		return "Nodely"
	default:
		return url
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
