// Command bootstrap feeds a sink from archive history then live algod via handoff.
//
//	go run ./cmd/bootstrap -archive ./archive -start N -sink postgres \
//	  -database "$DATABASE_URL" -algod "$VOI_ALGOD_URL"
//
// Historical rounds come only from the archive. Live rounds start at archive_tip+1.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	"github.com/NautilusOSS/voi-fast-follower/internal/metrics"
	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
	"github.com/NautilusOSS/voi-fast-follower/internal/stream"
	"github.com/NautilusOSS/voi-fast-follower/internal/voi"
)

func main() {
	archive := flag.String("archive", envOr("ARCHIVE_PATH", "./archive"), "archive root")
	start := flag.Uint64("start", 0, "start round (0 = sink checkpoint+1 or archive first)")
	end := flag.Uint64("end", 0, "end round inclusive (0 = follow live tip)")
	sinkName := flag.String("sink", "postgres", "postgres | archive | both")
	dsn := flag.String("database", envOr("DATABASE_URL", ""), "postgres DSN")
	outArchive := flag.String("out-archive", "", "output archive (archive/both)")
	migrations := flag.String("migrations", "migrations", "migrations path")
	batch := flag.Int("batch", 10, "commit batch size")
	algodURL := flag.String("algod", envOr("VOI_ALGOD_URL", ""), "algod URL for live handoff")
	token := flag.String("token", envOr("VOI_ALGOD_TOKEN", ""), "algod token")
	insertMode := flag.String("insert-mode", envOr("PG_INSERT_MODE", "unnest"), "postgres insert mode")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	hist, err := stream.OpenArchive(*archive)
	if err != nil {
		fatal("archive: %v", err)
	}
	defer hist.Close()

	archCP, archOK, err := hist.Checkpoint(ctx)
	if err != nil || !archOK {
		fatal("archive has no checkpoint")
	}

	var live stream.LiveFetcher
	if *algodURL != "" {
		c, err := voi.New(*algodURL, *token, slog.Default())
		if err != nil {
			fatal("algod: %v", err)
		}
		live = c
	}

	src := stream.NewHandoff(hist, live)
	m := metrics.Default()
	src.OnPhase(func(p stream.Phase) {
		m.SetBootstrapPhase(stream.PhaseMetricValue(p))
		if p == stream.PhaseHandoff {
			if hr, ok := src.HandoffRound(); ok {
				m.SetHandoffRound(hr)
			}
		}
		fmt.Printf("phase=%s\n", p)
	})

	pgURL, archPath := resolveSinks(*sinkName, *dsn, *outArchive)
	bundle, err := storage.BuildSinks(ctx, storage.BuildOptions{
		PostgresURL:        pgURL,
		PostgresMigrations: *migrations,
		PostgresInsertMode: *insertMode,
		ArchivePath:        archPath,
	})
	if err != nil {
		fatal("sink: %v", err)
	}
	defer bundle.Close()

	resume := *start
	if resume == 0 {
		if cp, ok, _ := bundle.Primary.LastProcessedRound(ctx); ok {
			resume = cp + 1
		} else {
			info, err := storage.InspectArchive(*archive)
			if err != nil || !info.HasData {
				fatal("cannot determine start round")
			}
			resume = info.FirstRound
		}
	}
	fmt.Printf("bootstrap resume=%d archive_tip=%d end=%v\n", resume, archCP, endLabel(*end))

	bs := *batch
	if bs < 1 {
		bs = 1
	}
	buf := make([]block.Block, 0, bs)
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		err := storage.AsBatchSink(bundle.Primary).CommitBatch(ctx, buf)
		buf = buf[:0]
		return err
	}

	t0 := time.Now()
	var n uint64
	round := resume
	for {
		if *end > 0 && round > *end {
			break
		}
		select {
		case <-ctx.Done():
			_ = flush()
			fatal("canceled: %v", ctx.Err())
		default:
		}

		blk, ok, err := src.Get(ctx, round)
		if err != nil {
			_ = flush()
			fatal("get %d: %v", round, err)
		}
		if !ok {
			if live == nil {
				_ = flush()
				fatal("round %d unavailable (no live source; archive tip=%d)", round, archCP)
			}
			// Wait for live tip / archive growth.
			time.Sleep(200 * time.Millisecond)
			continue
		}
		if round <= archCP {
			m.RecordArchiveReplay()
		}
		buf = append(buf, blk)
		n++
		round++
		if len(buf) >= bs || (live != nil && round > archCP && len(buf) >= 1) {
			// Live path: smaller batches for lower latency after handoff.
			if round > archCP+1 {
				bs = 1
			}
			if err := flush(); err != nil {
				fatal("commit: %v", err)
			}
		}
		if *end == 0 && live != nil {
			tip, err := live.LastRound(ctx)
			if err == nil && round > tip && len(buf) == 0 {
				// Caught tip; keep following until signal.
				time.Sleep(500 * time.Millisecond)
			}
		}
	}
	if err := flush(); err != nil {
		fatal("commit: %v", err)
	}
	elapsed := time.Since(t0)
	fmt.Printf("done blocks=%d elapsed=%s bps=%.1f final_round=%d\n",
		n, elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds(), round-1)
}

func resolveSinks(name, dsn, outArch string) (pg, arch string) {
	switch strings.ToLower(name) {
	case "postgres":
		if dsn == "" {
			fatal("DATABASE_URL required")
		}
		return dsn, ""
	case "archive":
		if outArch == "" {
			fatal("-out-archive required")
		}
		return "", outArch
	case "both":
		if dsn == "" || outArch == "" {
			fatal("both requires -database and -out-archive")
		}
		return dsn, outArch
	default:
		fatal("unknown sink %q", name)
		return "", ""
	}
}

func endLabel(end uint64) string {
	if end == 0 {
		return "live"
	}
	return fmt.Sprintf("%d", end)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func fatal(f string, a ...any) {
	fmt.Fprintf(os.Stderr, f+"\n", a...)
	os.Exit(1)
}
