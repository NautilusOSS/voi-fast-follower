// Command block-consumer is a minimal reference consumer for Fast Follower archives.
//
// Historical (no Voi network):
//
//	go run ./examples/block-consumer \
//	  --archive ./archive --start 1000000 --end 1000100 \
//	  --checkpoint ./consumer.cp
//
// Follow growing archive (follower still acquiring):
//
//	go run ./examples/block-consumer \
//	  --archive ./archive --follow --checkpoint ./consumer.cp
//
// Historical → live via algod after archive tip:
//
//	go run ./examples/block-consumer \
//	  --archive ./archive --follow --algod http://127.0.0.1:4001 \
//	  --token aaa... --checkpoint ./consumer.cp
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/NautilusOSS/voi-fast-follower/pkg/consumer"
)

func main() {
	archive := flag.String("archive", envOr("ARCHIVE_PATH", "./archive"), "follower archive directory")
	start := flag.Uint64("start", 0, "start round (0 = checkpoint+1 or archive first)")
	end := flag.Uint64("end", 0, "end round inclusive (0 = follow until cancel)")
	follow := flag.Bool("follow", false, "wait for new archive rounds (and optional algod live)")
	checkpoint := flag.String("checkpoint", "./consumer.checkpoint", "consumer cursor file (independent from follower)")
	algod := flag.String("algod", envOr("VOI_ALGOD_URL", ""), "optional algod for live after archive tip")
	token := flag.String("token", envOr("VOI_ALGOD_TOKEN", ""), "algod token")
	poll := flag.Duration("poll", 200*time.Millisecond, "poll interval when following archive")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	arch, err := consumer.OpenArchive(*archive)
	if err != nil {
		fatal("open archive: %v", err)
	}
	defer arch.Close()

	cp := consumer.CheckpointFile{Path: *checkpoint}
	resume, err := cp.ResumeRound(*start)
	if err != nil {
		fatal("checkpoint: %v", err)
	}
	if resume == 0 && *start == 0 {
		fatal("set --start or provide a consumer checkpoint")
	}
	if resume > 0 && (*start == 0 || resume > *start) {
		fmt.Printf("resume from consumer checkpoint round %d\n", resume)
	} else if *start > 0 {
		resume = *start
	}

	var src consumer.Source = arch
	var handoff *consumer.Handoff
	if *follow && *algod != "" {
		live, err := consumer.OpenAlgod(*algod, *token)
		if err != nil {
			fatal("algod: %v", err)
		}
		handoff = consumer.NewHandoff(arch, live)
		handoff.OnPhase(func(p consumer.Phase) {
			fmt.Printf("phase=%s\n", p)
		})
		src = handoff
	}

	mode := "historical"
	if *follow {
		mode = "follow"
	}
	fmt.Printf("block-consumer mode=%s archive=%s start=%d end=%d\n", mode, *archive, resume, *end)

	var count uint64
	_, err = consumer.Process(ctx, consumer.ProcessOptions{
		Source:     src,
		Start:      resume,
		End:        *end,
		Checkpoint: cp,
		Archive:    arch,
		WaitPoll:   *poll,
		OnBlock: func(b consumer.Block) error {
			fmt.Printf("round=%d hash=%s prev=%s txns=%d raw_bytes=%d\n",
				b.Round, b.BlockHash, b.PreviousBlockHash, b.TxnCount, len(b.Raw))
			count++
			return nil
		},
	})
	if err != nil && err != context.Canceled {
		fatal("process: %v", err)
	}
	fmt.Printf("done processed=%d consumer_checkpoint=", count)
	if r, ok, _ := cp.Load(); ok {
		fmt.Println(r)
	} else {
		fmt.Println("none")
	}
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
