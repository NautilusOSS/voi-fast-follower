// Command archive-info prints lightweight metadata about a local archive.
//
//	go run ./cmd/archive-info -archive ./archive
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/storage"
)

func main() {
	archive := flag.String("archive", envOr("ARCHIVE_PATH", "./archive"), "archive root")
	flag.Parse()

	info, err := storage.InspectArchive(*archive)
	if err != nil {
		fmt.Fprintf(os.Stderr, "archive-info: %v\n", err)
		os.Exit(1)
	}
	cp := "none"
	if info.HasCheckpoint {
		cp = fmt.Sprintf("%d", info.Checkpoint)
	}
	first, last := "-", "-"
	if info.HasData {
		first = fmt.Sprintf("%d", info.FirstRound)
		last = fmt.Sprintf("%d", info.LastRound)
	}
	fmt.Printf(`Archive:      %s
First round:  %s
Last round:   %s
Checkpoint:   %s
Segments:     %d
Blocks:       %d
Bytes:        %d
Segment size: %d
Version:      %d
`, info.Root, first, last, cp, info.Segments, info.Blocks, info.Bytes, info.SegmentSize, info.Version)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
