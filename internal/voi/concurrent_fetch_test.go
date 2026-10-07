package voi

import (
	"context"
	"os"
	"sync"
	"testing"
)

func TestConcurrentFetchRounds(t *testing.T) {
	if os.Getenv("VOI_INTEGRATION") == "" {
		t.Skip("set VOI_INTEGRATION=1")
	}
	url := os.Getenv("VOI_ALGOD_URL")
	if url == "" {
		url = "https://mainnet-api.voi.nodely.dev"
	}
	c, err := New(url, os.Getenv("VOI_ALGOD_TOKEN"), nil)
	if err != nil {
		t.Fatal(err)
	}
	tip, err := c.LastRound(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	from := tip - 40
	const n = 32
	out := make([]uint64, n)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := from + uint64(i)
			blk, err := c.FetchBlock(context.Background(), r)
			if err != nil {
				errCh <- err
				return
			}
			mu.Lock()
			out[i] = blk.Round
			mu.Unlock()
			if blk.Round != r {
				errCh <- errRoundMismatch{want: r, got: blk.Round}
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		want := from + uint64(i)
		if out[i] != want {
			t.Fatalf("slot %d want %d got %d", i, want, out[i])
		}
	}
}

type errRoundMismatch struct{ want, got uint64 }

func (e errRoundMismatch) Error() string {
	return "round mismatch"
}
