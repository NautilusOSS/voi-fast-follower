// Command archive-export copies a durable round range into a new archive directory.
//
//	go run ./cmd/archive-export -archive ./archive -out ./bundle -start N -end M
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
)

func main() {
	src := flag.String("archive", envOr("ARCHIVE_PATH", "./archive"), "source archive")
	out := flag.String("out", "", "destination archive root (must be empty)")
	start := flag.Uint64("start", 0, "start round")
	end := flag.Uint64("end", 0, "end round")
	flag.Parse()
	if *out == "" || *start == 0 || *end == 0 || *end < *start {
		fmt.Fprintf(os.Stderr, `archive-export — copy a durable round range into a new archive directory

usage:
  go run ./cmd/archive-export -archive DIR -out DEST -start N -end M

DEST must be empty. Verifies the export after write.
`)
		os.Exit(2)
	}
	t0 := time.Now()
	if err := storage.ExportArchive(context.Background(), *src, *out, *start, *end); err != nil {
		fmt.Fprintf(os.Stderr, "export: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("exported %d–%d → %s in %s\n", *start, *end, *out, time.Since(t0).Round(time.Millisecond))
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
