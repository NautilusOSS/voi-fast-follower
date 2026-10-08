// Command archive-prune applies rolling retention (keep last N rounds).
//
//	go run ./cmd/archive-prune -archive ./archive -keep-rounds 500000
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
)

func main() {
	root := flag.String("archive", envOr("ARCHIVE_PATH", "./archive"), "archive root")
	keep := flag.Uint64("keep-rounds", 0, "retain the last N rounds through checkpoint")
	keepFrom := flag.Uint64("keep-from", 0, "alternative: absolute first round to retain")
	flag.Parse()

	info, err := storage.InspectArchive(*root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "inspect: %v\n", err)
		os.Exit(1)
	}
	if !info.HasCheckpoint {
		fmt.Fprintf(os.Stderr, "no checkpoint\n")
		os.Exit(1)
	}
	from := *keepFrom
	if from == 0 {
		if *keep == 0 {
			fmt.Fprintf(os.Stderr, "usage: archive-prune -archive DIR -keep-rounds N\n")
			os.Exit(2)
		}
		if info.Checkpoint+1 <= *keep {
			fmt.Printf("nothing to prune (checkpoint=%d keep=%d)\n", info.Checkpoint, *keep)
			return
		}
		from = info.Checkpoint - *keep + 1
	}
	fmt.Printf("pruning archive %s keep-from=%d checkpoint=%d\n", *root, from, info.Checkpoint)
	if err := storage.PruneArchive(context.Background(), *root, from); err != nil {
		fmt.Fprintf(os.Stderr, "prune: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("ok")
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
