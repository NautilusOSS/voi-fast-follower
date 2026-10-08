// Command archive-import verifies and copies a source archive into an empty destination.
//
//	go run ./cmd/archive-import -in ./bundle -archive ./archive-new
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
	in := flag.String("in", "", "source archive / bundle")
	dest := flag.String("archive", "", "destination archive root (must be empty)")
	flag.Parse()
	if *in == "" || *dest == "" {
		fmt.Fprintf(os.Stderr, "usage: archive-import -in SRC -archive DEST\n")
		os.Exit(2)
	}
	t0 := time.Now()
	if err := storage.ImportArchive(context.Background(), *in, *dest); err != nil {
		fmt.Fprintf(os.Stderr, "import: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("imported %s → %s in %s\n", *in, *dest, time.Since(t0).Round(time.Millisecond))
}
