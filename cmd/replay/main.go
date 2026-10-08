// Command replay feeds archived raw blocks into a sink without contacting Voi.
//
// Example:
//
//	go run ./cmd/replay -archive ./archive -start 100000 -end 101000 -sink postgres \
//	  -database "$DATABASE_URL"
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/storage"
)

func main() {
	archive := flag.String("archive", envOr("ARCHIVE_PATH", "./archive"), "archive root directory")
	start := flag.Uint64("start", 0, "start round (inclusive)")
	end := flag.Uint64("end", 0, "end round (inclusive)")
	sinkName := flag.String("sink", "postgres", "destination sink: postgres | archive | both")
	dsn := flag.String("database", envOr("DATABASE_URL", ""), "postgres DSN (postgres/both)")
	outArchive := flag.String("out-archive", "", "output archive path (archive/both)")
	migrations := flag.String("migrations", "migrations", "SQL migrations path")
	batch := flag.Int("batch", 10, "commit batch size")
	insertMode := flag.String("insert-mode", envOr("PG_INSERT_MODE", "unnest"), "postgres insert mode")
	flag.Parse()

	if *start == 0 || *end == 0 || *end < *start {
		fmt.Fprintf(os.Stderr, "usage: replay -archive DIR -start N -end M -sink postgres|archive|both\n")
		os.Exit(2)
	}

	ctx := context.Background()
	fmt.Printf("reading archive %s rounds %d-%d\n", *archive, *start, *end)
	t0 := time.Now()
	blocks, err := storage.ReadArchiveBlocks(*archive, *start, *end)
	if err != nil {
		fmt.Fprintf(os.Stderr, "read archive: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("loaded %d blocks in %s\n", len(blocks), time.Since(t0).Round(time.Millisecond))

	// Validate contiguous linkage once more before sink write.
	for i := 1; i < len(blocks); i++ {
		if err := block.ValidateLinkage(blocks[i-1].BlockHash, blocks[i]); err != nil {
			fmt.Fprintf(os.Stderr, "linkage: %v\n", err)
			os.Exit(1)
		}
	}

	pgURL := ""
	archPath := ""
	switch strings.ToLower(*sinkName) {
	case "postgres":
		if *dsn == "" {
			fmt.Fprintf(os.Stderr, "DATABASE_URL / -database required for postgres sink\n")
			os.Exit(2)
		}
		pgURL = *dsn
	case "archive":
		if *outArchive == "" {
			fmt.Fprintf(os.Stderr, "-out-archive required for archive sink\n")
			os.Exit(2)
		}
		archPath = *outArchive
	case "both":
		if *dsn == "" || *outArchive == "" {
			fmt.Fprintf(os.Stderr, "both requires -database and -out-archive\n")
			os.Exit(2)
		}
		pgURL = *dsn
		archPath = *outArchive
	default:
		fmt.Fprintf(os.Stderr, "unknown sink %q\n", *sinkName)
		os.Exit(2)
	}

	bundle, err := storage.BuildSinks(ctx, storage.BuildOptions{
		PostgresURL:        pgURL,
		PostgresMigrations: *migrations,
		PostgresInsertMode: *insertMode,
		ArchivePath:        archPath,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "sink: %v\n", err)
		os.Exit(1)
	}
	defer bundle.Close()

	t1 := time.Now()
	bs := *batch
	if bs < 1 {
		bs = 1
	}
	for i := 0; i < len(blocks); i += bs {
		j := i + bs
		if j > len(blocks) {
			j = len(blocks)
		}
		if err := bundle.Primary.CommitBatch(ctx, blocks[i:j]); err != nil {
			fmt.Fprintf(os.Stderr, "commit %d-%d: %v\n", blocks[i].Round, blocks[j-1].Round, err)
			os.Exit(1)
		}
	}
	elapsed := time.Since(t1)
	bps := float64(len(blocks)) / elapsed.Seconds()
	fmt.Printf("replayed %d blocks → %s in %s (%.2f blk/s)\n",
		len(blocks), strings.Join(bundle.Names, "+"), elapsed.Round(time.Millisecond), bps)

	cp, ok, err := bundle.Primary.LastProcessedRound(ctx)
	if err != nil || !ok || cp < *end {
		fmt.Fprintf(os.Stderr, "checkpoint=%d ok=%v err=%v want >= %d\n", cp, ok, err, *end)
		os.Exit(1)
	}
	fmt.Printf("checkpoint=%d\n", cp)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
