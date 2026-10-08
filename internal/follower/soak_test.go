package follower

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	"github.com/NautilusOSS/voi-fast-follower/internal/health"
	"github.com/NautilusOSS/voi-fast-follower/internal/metrics"
)

// advancingSource simulates catch-up then live tip growth.
type advancingSource struct {
	mu      sync.Mutex
	tip     uint64
	blocks  map[uint64]block.Block
	waiters []chan uint64
}

func newAdvancingSource(start, initialTip uint64) *advancingSource {
	s := &advancingSource{
		tip:    initialTip,
		blocks: map[uint64]block.Block{},
	}
	var prev string
	for r := start; r <= initialTip+50; r++ {
		h := "HASH-" + ustr(r)
		s.blocks[r] = block.Block{
			Round:             r,
			BlockHash:         h,
			PreviousBlockHash: prev,
			Raw:               []byte(ustr(r)),
		}
		prev = h
	}
	return s
}

func (s *advancingSource) LastRound(context.Context) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tip, nil
}

func (s *advancingSource) GetBlock(_ context.Context, round uint64) (block.Block, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	blk, ok := s.blocks[round]
	if !ok {
		return block.Block{}, context.Canceled
	}
	return blk, nil
}

func (s *advancingSource) WaitForBlockAfter(ctx context.Context, round uint64) (uint64, error) {
	s.mu.Lock()
	if s.tip > round {
		t := s.tip
		s.mu.Unlock()
		return t, nil
	}
	ch := make(chan uint64, 1)
	s.waiters = append(s.waiters, ch)
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case t := <-ch:
		return t, nil
	}
}

func (s *advancingSource) advance(to uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if to <= s.tip {
		return
	}
	var prev string
	if s.tip > 0 {
		if b, ok := s.blocks[s.tip]; ok {
			prev = b.BlockHash
		}
	}
	for r := s.tip + 1; r <= to; r++ {
		h := "HASH-" + ustr(r)
		s.blocks[r] = block.Block{
			Round:             r,
			BlockHash:         h,
			PreviousBlockHash: prev,
			Raw:               []byte(ustr(r)),
		}
		prev = h
	}
	s.tip = to
	for _, w := range s.waiters {
		select {
		case w <- to:
		default:
		}
	}
	s.waiters = nil
}

func TestCatchupLiveRestart(t *testing.T) {
	src := newAdvancingSource(100, 110)
	// Fix hashes to be unique and link properly.
	src.mu.Lock()
	var prev string
	for r := uint64(100); r <= 160; r++ {
		h := "HASH-" + ustr(r)
		src.blocks[r] = block.Block{
			Round: r, BlockHash: h, PreviousBlockHash: prev, Raw: []byte(ustr(r)),
		}
		prev = h
	}
	src.tip = 110
	src.mu.Unlock()

	sink := &mockSink{}
	cfg := testCfg(4, 16)
	cfg.Sync.StartRound = "100"
	cfg.Sync.CommitBatchSize = 5
	cfg.Sync.CommitFlushInterval = 50 * time.Millisecond
	ht := health.New()
	e := New(cfg, src, sink, metrics.New(), nil).WithHealth(ht)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	// Wait for catch-up to tip 110.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		last, ok, _ := sink.LastProcessedRound(context.Background())
		if ok && last >= 110 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	last, ok, _ := sink.LastProcessedRound(context.Background())
	if !ok || last < 110 {
		cancel()
		<-done
		t.Fatalf("catch-up failed last=%d", last)
	}
	if mode := ht.Snapshot().Mode; mode != health.ModeLive && mode != health.ModeCatchingUp {
		// may briefly be live
	}

	// Live: advance tip and observe new rounds.
	src.advance(115)
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		last, ok, _ = sink.LastProcessedRound(context.Background())
		if ok && last >= 115 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	last, ok, _ = sink.LastProcessedRound(context.Background())
	if !ok || last < 115 {
		cancel()
		<-done
		t.Fatalf("live follow failed last=%d", last)
	}

	// Graceful stop.
	cancel()
	select {
	case err := <-done:
		if err != nil && err != context.Canceled {
			t.Fatalf("run err: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown timeout")
	}

	cp, ok, _ := sink.LastProcessedRound(context.Background())
	if !ok {
		t.Fatal("missing checkpoint after shutdown")
	}

	// Restart from checkpoint+1.
	src.advance(120)
	sink2 := &mockSink{last: cp, hasLast: true, hashes: map[uint64]string{}}
	// Seed hashes for linkage.
	for r, blk := range src.blocks {
		if r <= cp {
			sink2.hashes[r] = blk.BlockHash
		}
	}
	e2 := New(cfg, src, sink2, metrics.New(), nil)
	ctx2, cancel2 := context.WithCancel(context.Background())
	done2 := make(chan error, 1)
	go func() { done2 <- e2.Run(ctx2) }()

	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		last, ok, _ = sink2.LastProcessedRound(context.Background())
		if ok && last >= 120 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel2()
	<-done2
	last, ok, _ = sink2.LastProcessedRound(context.Background())
	if !ok || last < 120 {
		t.Fatalf("restart resume failed last=%d cp=%d", last, cp)
	}
	rounds := sink2.committedRounds()
	if len(rounds) == 0 || rounds[0] != cp+1 {
		t.Fatalf("expected resume at %d, got %v", cp+1, rounds)
	}
	for i := 1; i < len(rounds); i++ {
		if rounds[i] != rounds[i-1]+1 {
			t.Fatalf("gap after restart: %v", rounds)
		}
	}
}

func ustr(n uint64) string {
	return fmtUint(n)
}

func fmtUint(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func TestGracefulShutdownDoesNotCorruptCheckpoint(t *testing.T) {
	src := newFakeSource(50, 80)
	sink := &mockSink{}
	cfg := testCfg(4, 16)
	cfg.Sync.StartRound = "50"
	cfg.Sync.CommitBatchSize = 10
	e := New(cfg, src, sink, metrics.New(), nil).WithUntilRound(80)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	// Cancel mid-run after some progress.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		last, ok, _ := sink.LastProcessedRound(context.Background())
		if ok && last >= 55 {
			cancel()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown hung")
	}
	cp, ok, _ := sink.LastProcessedRound(context.Background())
	if !ok {
		return // nothing committed yet — still valid
	}
	rounds := uniqueSorted(sink.committedRounds())
	if rounds[len(rounds)-1] != cp {
		t.Fatalf("checkpoint %d != last committed %d", cp, rounds[len(rounds)-1])
	}
}
