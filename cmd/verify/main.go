// Command verify validates a local archive without contacting Voi.
//
//	go run ./cmd/verify -archive ./archive
//	go run ./cmd/verify -archive ./archive -start 100000 -end 109999
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/storage"
)

func main() {
	archive := flag.String("archive", envOr("ARCHIVE_PATH", "./archive"), "archive root")
	start := flag.Uint64("start", 0, "optional start round (0 = beginning)")
	end := flag.Uint64("end", 0, "optional end round (0 = checkpoint)")
	flag.Parse()

	rep, err := storage.VerifyArchive(*archive, *start, *end)
	if err != nil {
		fmt.Fprintf(os.Stderr, "verify: %v\n", err)
		os.Exit(1)
	}
	fmt.Print(storage.FormatVerifySummary(rep))
	if len(rep.Errors) > 0 && !rep.OK {
		for _, e := range rep.Errors {
			fmt.Fprintf(os.Stderr, "  - %s\n", e)
		}
	}
	if !rep.OK {
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
