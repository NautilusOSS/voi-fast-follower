package voi

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
)

// TestConcurrentRawThenSerialDecode isolates whether HTTP or decode corrupts rounds.
func TestConcurrentRawThenSerialDecode(t *testing.T) {
	if os.Getenv("VOI_INTEGRATION") == "" {
		t.Skip("set VOI_INTEGRATION=1")
	}
	url := os.Getenv("VOI_ALGOD_URL")
	if url == "" {
		url = "https://mainnet-api.voi.nodely.dev"
	}
	ac, err := algod.MakeClient(url, os.Getenv("VOI_ALGOD_TOKEN"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := ac.Status().Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	from := st.LastRound - 40
	const n = 32
	raws := make([][]byte, n)
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := from + uint64(i)
			raw, err := ac.BlockRaw(r).Do(context.Background())
			if err != nil {
				errCh <- err
				return
			}
			raws[i] = raw
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		blk, err := block.DecodeRaw(raws[i])
		if err != nil {
			t.Fatal(err)
		}
		want := from + uint64(i)
		if blk.Round != want {
			t.Fatalf("HTTP/decode mismatch slot=%d want=%d got=%d rawLen=%d", i, want, blk.Round, len(raws[i]))
		}
	}
}
