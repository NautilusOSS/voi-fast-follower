// Command archive-compare checks semantic equality of two archives over a range.
//
//	go run ./cmd/archive-compare -a ./arch1 -b ./arch2 -start N -end M
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
)

func main() {
	a := flag.String("a", "", "archive A")
	b := flag.String("b", "", "archive B")
	start := flag.Uint64("start", 0, "start round")
	end := flag.Uint64("end", 0, "end round")
	flag.Parse()
	if *a == "" || *b == "" || *start == 0 || *end == 0 || *end < *start {
		fmt.Fprintf(os.Stderr, "usage: archive-compare -a DIR -b DIR -start N -end M\n")
		os.Exit(2)
	}
	rep, err := storage.CompareArchives(context.Background(), *a, *b, *start, *end)
	if err != nil {
		fmt.Fprintf(os.Stderr, "compare: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("range=%d–%d blocks=%d ok=%v\n", rep.Start, rep.End, rep.Blocks, rep.OK)
	if rep.HasDiff {
		fmt.Printf("first_diff=%d reason=%s\n", rep.FirstDiff, rep.DiffReason)
	}
	if !rep.OK {
		os.Exit(1)
	}
}
