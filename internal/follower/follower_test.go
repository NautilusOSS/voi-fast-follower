package follower

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	"github.com/NautilusOSS/voi-fast-follower/internal/config"
	"github.com/NautilusOSS/voi-fast-follower/internal/metrics"
	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
)

// --- fakes -----------------------------------------------------------------

type mockSink struct {
	mu         sync.Mutex
	rounds     []uint64
	last       uint64
	hasLast    bool
	hashes     map[uint64]string
	commitErr  error
	failOnce   atomic.Bool
	commits    atomic.Int64 // CommitBatch calls
	batchSizes []int
}

func (m *mockSink) Commit(ctx context.Context, blk block.Block) error {
	return m.CommitBatch(ctx, []block.Block{blk})
}

func (m *mockSink) CommitBatch(_ context.Context, blocks []block.Block) error {
	if err := storage.ValidateBatch(blocks); err != nil {
		return err
	}
	m.commits.Add(1)
	if m.failOnce.CompareAndSwap(true, false) {
		return errors.New("injected commit failure")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.commitErr != nil {
		return m.commitErr
	}
	m.batchSizes = append(m.batchSizes, len(blocks))
	if m.hashes == nil {
		m.hashes = map[uint64]string{}
	}
	for _, blk := range blocks {
		m.rounds = append(m.rounds, blk.Round)
		m.hashes[blk.Round] = blk.BlockHash
		if !m.hasLast || blk.Round >= m.last {
			m.last = blk.Round
			m.hasLast = true
		}
	}
	return nil
}

func (m *mockSink) LastProcessedRound(_ context.Context) (uint64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.last, m.hasLast, nil
}

func (m *mockSink) MaxBlockRound(_ context.Context) (uint64, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.hasLast {
		return 0, false, nil
	}
	return m.last, true, nil
}

func (m *mockSink) BlockHash(_ context.Context, round uint64) (string, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.hashes[round]
	return h, ok, nil
}

func (m *mockSink) Close() error { return nil }

func (m *mockSink) committedRounds() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]uint64, len(m.rounds))
	copy(out, m.rounds)
	return out
}

func (m *mockSink) recordedBatchSizes() []int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]int, len(m.batchSizes))
	copy(out, m.batchSizes)
	return out
}

type fakeSource struct {
	mu          sync.Mutex
	tip         uint64
	blocks      map[uint64]block.Block
	delay       map[uint64]time.Duration
	failRounds  map[uint64]int
	getCalls    atomic.Int64
	maxInFlight atomic.Int64
	inFlight    atomic.Int64
	waitCh      chan struct{}
}

func newFakeSource(from, to uint64) *fakeSource {
	blocks := make(map[uint64]block.Block)
	var prev string
	for r := from; r <= to; r++ {
		hash := fmt.Sprintf("H%d", r)
		blocks[r] = block.Block{
			Round:             r,
			BlockHash:         hash,
			PreviousBlockHash: prev,
			Timestamp:         int64(r),
			Raw:               []byte(fmt.Sprintf("raw-%d", r)),
		}
		prev = hash
	}
	return &fakeSource{
		tip:        to,
		blocks:     blocks,
		delay:      map[uint64]time.Duration{},
		failRounds: map[uint64]int{},
	}
}

func (f *fakeSource) GetBlock(ctx context.Context, round uint64) (block.Block, error) {
	f.getCalls.Add(1)
	cur := f.inFlight.Add(1)
	for {
		old := f.maxInFlight.Load()
		if cur <= old || f.maxInFlight.CompareAndSwap(old, cur) {
			break
		}
	}
	defer f.inFlight.Add(-1)

	f.mu.Lock()
	d := f.delay[round]
	fails := f.failRounds[round]
	if fails > 0 {
		f.failRounds[round] = fails - 1
		f.mu.Unlock()
		return block.Block{}, fmt.Errorf("injected fetch fail round %d", round)
	}
	blk, ok := f.blocks[round]
	f.mu.Unlock()

	if d > 0 {
		select {
		case <-ctx.Done():
			return block.Block{}, ctx.Err()
		case <-time.After(d):
		}
	}
	if !ok {
		return block.Block{}, fmt.Errorf("missing block %d", round)
	}
	return blk, nil
}

func (f *fakeSource) LastRound(_ context.Context) (uint64, error) {
	f.mu.Lock()
	tip := f.tip
	f.mu.Unlock()
	return tip, nil
}

func (f *fakeSource) WaitForBlockAfter(ctx context.Context, round uint64) (uint64, error) {
	if f.waitCh != nil {
		select {
		case f.waitCh <- struct{}{}:
		default:
		}
	}
	f.mu.Lock()
	tip := f.tip
	f.mu.Unlock()
	if tip > round {
		return tip, nil
	}
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
			f.mu.Lock()
			tip = f.tip
			f.mu.Unlock()
			if tip > round {
				return tip, nil
			}
		}
	}
}

func (f *fakeSource) setTip(tip uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tip = tip
}

func (f *fakeSource) addBlock(blk block.Block) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.blocks[blk.Round] = blk
	if blk.Round > f.tip {
		f.tip = blk.Round
	}
}

func testCfg(workers, window int) *config.Config {
	return &config.Config{
		Sync: config.SyncConfig{
			StartRound:          "100",
			Mode:                "fast",
			PollInterval:        10 * time.Millisecond,
			Workers:             workers,
			FetchWindow:         window,
			CommitBatchSize:     1,
			CommitFlushInterval: 0,
		},
	}
}

func runUntil(t *testing.T, e *Engine, sink *mockSink, endRound uint64, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		last, ok, _ := sink.LastProcessedRound(context.Background())
		if ok && last >= endRound {
			cancel()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	err := <-done
	t.Fatalf("timeout waiting for round %d; last=%v err=%v commits=%v",
		endRound, sink.committedRounds(), err, sink.committedRounds())
}

// --- tests -----------------------------------------------------------------

func TestResolveStartRoundPrefersCheckpoint(t *testing.T) {
	cfg := &config.Config{Sync: config.SyncConfig{StartRound: "100"}}
	sink := &mockSink{last: 200, hasLast: true}
	e := New(cfg, nil, sink, metrics.New(), nil)
	next, err := e.resolveStartRound(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next != 201 {
		t.Fatalf("next=%d want 201", next)
	}
}

func TestSequentialIngestion(t *testing.T) {
	src := newFakeSource(100, 110)
	sink := &mockSink{}
	cfg := testCfg(1, 1)
	cfg.Sync.StartRound = "100"
	e := New(cfg, src, sink, metrics.New(), nil)
	runUntil(t, e, sink, 110, 5*time.Second)

	rounds := sink.committedRounds()
	if len(rounds) < 11 {
		t.Fatalf("got %d commits want >= 11: %v", len(rounds), rounds)
	}
	for i := 0; i < 11; i++ {
		if rounds[i] != 100+uint64(i) {
			t.Fatalf("order broken at %d: %v", i, rounds)
		}
	}
}

func TestOutOfOrderFetchOrderedCommit(t *testing.T) {
	src := newFakeSource(100, 110)
	src.delay[100] = 80 * time.Millisecond
	src.delay[101] = 60 * time.Millisecond
	src.delay[102] = 40 * time.Millisecond

	sink := &mockSink{}
	cfg := testCfg(8, 16)
	cfg.Sync.StartRound = "100"
	e := New(cfg, src, sink, metrics.New(), nil)
	runUntil(t, e, sink, 110, 5*time.Second)

	rounds := sink.committedRounds()
	for i := 1; i < len(rounds); i++ {
		if rounds[i] != rounds[i-1]+1 {
			t.Fatalf("non-contiguous commit order: %v", rounds)
		}
	}
	if rounds[0] != 100 {
		t.Fatalf("first commit=%d want 100", rounds[0])
	}
}

func TestMissingRoundNeverSkipped(t *testing.T) {
	src := newFakeSource(100, 105)
	src.failRounds[102] = 2

	sink := &mockSink{}
	cfg := testCfg(4, 8)
	cfg.Sync.StartRound = "100"
	cfg.Sync.PollInterval = 5 * time.Millisecond
	e := New(cfg, src, sink, metrics.New(), nil)
	runUntil(t, e, sink, 105, 5*time.Second)

	rounds := uniqueSorted(sink.committedRounds())
	for i, want := range []uint64{100, 101, 102, 103, 104, 105} {
		if i >= len(rounds) || rounds[i] != want {
			t.Fatalf("rounds=%v missing contiguous sequence", rounds)
		}
	}
}

func TestDuplicateBlockIdempotent(t *testing.T) {
	sink := &mockSink{}
	blk := block.Block{Round: 1, BlockHash: "H1", PreviousBlockHash: "", Raw: []byte{1}}
	if err := sink.Commit(context.Background(), blk); err != nil {
		t.Fatal(err)
	}
	if err := sink.Commit(context.Background(), blk); err != nil {
		t.Fatal(err)
	}
	if sink.commits.Load() != 2 {
		t.Fatalf("commits=%d want 2", sink.commits.Load())
	}
}

func TestRestartFromCheckpoint(t *testing.T) {
	src := newFakeSource(100, 120)
	sink := &mockSink{last: 109, hasLast: true, hashes: map[uint64]string{109: "H109"}}
	cfg := testCfg(4, 8)
	cfg.Sync.StartRound = "100"
	e := New(cfg, src, sink, metrics.New(), nil)

	next, err := e.resolveStartRound(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if next != 110 {
		t.Fatalf("resume=%d want 110", next)
	}

	runUntil(t, e, sink, 120, 5*time.Second)
	rounds := sink.committedRounds()
	if rounds[0] != 110 {
		t.Fatalf("first after restart=%d want 110 (got %v)", rounds[0], rounds)
	}
}

func TestCrashRetryCommit(t *testing.T) {
	src := newFakeSource(50, 55)
	sink := &mockSink{}
	sink.failOnce.Store(true)
	cfg := testCfg(2, 4)
	cfg.Sync.StartRound = "50"
	e := New(cfg, src, sink, metrics.New(), nil)
	runUntil(t, e, sink, 55, 5*time.Second)

	rounds := uniqueSorted(sink.committedRounds())
	for i, want := range []uint64{50, 51, 52, 53, 54, 55} {
		if i >= len(rounds) || rounds[i] != want {
			t.Fatalf("after retry rounds=%v", rounds)
		}
	}
}

func TestPreviousBlockLinkage(t *testing.T) {
	err := validateBlock(block.Block{
		Round:             11,
		PreviousBlockHash: "BBBB",
		BlockHash:         "CCCC",
		Raw:               []byte{1},
	}, "AAAA", true)
	if err == nil {
		t.Fatal("expected chain break error")
	}
}

func TestPreviousBlockLinkageOK(t *testing.T) {
	err := validateBlock(block.Block{
		Round:             11,
		PreviousBlockHash: "AAAA",
		BlockHash:         "CCCC",
		Raw:               []byte{1},
	}, "AAAA", true)
	if err != nil {
		t.Fatal(err)
	}
}

func TestPostgresTransactionFailureSurfaced(t *testing.T) {
	sink := &mockSink{commitErr: errors.New("pg tx failed")}
	err := sink.Commit(context.Background(), block.Block{
		Round: 1, BlockHash: "H", Raw: []byte{1},
	})
	if err == nil || err.Error() != "pg tx failed" {
		t.Fatalf("err=%v", err)
	}
}

func TestWorkerConcurrency(t *testing.T) {
	src := newFakeSource(1000, 1031)
	for r := uint64(1000); r <= 1031; r++ {
		src.delay[r] = 30 * time.Millisecond
	}
	sink := &mockSink{}
	cfg := testCfg(8, 16)
	cfg.Sync.StartRound = "1000"
	e := New(cfg, src, sink, metrics.New(), nil)
	runUntil(t, e, sink, 1031, 10*time.Second)

	if src.maxInFlight.Load() < 4 {
		t.Fatalf("max in-flight fetches=%d; expected concurrent workers", src.maxInFlight.Load())
	}
}

func TestBoundedBuffering(t *testing.T) {
	src := newFakeSource(200, 260)
	src.delay[200] = 150 * time.Millisecond

	sink := &mockSink{}
	const window = 4
	cfg := testCfg(4, window)
	cfg.Sync.StartRound = "200"
	e := New(cfg, src, sink, metrics.New(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	time.Sleep(80 * time.Millisecond)
	calls := src.getCalls.Load()
	if calls > int64(window*2) {
		cancel()
		<-done
		t.Fatalf("getCalls=%d exceeds bounded window=%d", calls, window)
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		last, ok, _ := sink.LastProcessedRound(context.Background())
		if ok && last >= 210 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
}

func TestLiveFollowSameCommitPath(t *testing.T) {
	src := newFakeSource(10, 12)
	sink := &mockSink{}
	cfg := testCfg(2, 4)
	cfg.Sync.StartRound = "10"
	cfg.Sync.CommitBatchSize = 50 // live path must still flush with size 1
	e := New(cfg, src, sink, metrics.New(), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		last, ok, _ := sink.LastProcessedRound(context.Background())
		if ok && last >= 12 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	src.addBlock(block.Block{
		Round:             13,
		BlockHash:         "H13",
		PreviousBlockHash: "H12",
		Raw:               []byte("raw-13"),
	})
	src.setTip(13)

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		last, ok, _ := sink.LastProcessedRound(context.Background())
		if ok && last >= 13 {
			cancel()
			<-done
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	t.Fatalf("live follow did not commit round 13; rounds=%v", sink.committedRounds())
}

func TestBatchCommitsPreserveOrdering(t *testing.T) {
	src := newFakeSource(100, 130)
	sink := &mockSink{}
	cfg := testCfg(8, 64)
	cfg.Sync.StartRound = "100"
	cfg.Sync.CommitBatchSize = 10
	cfg.Sync.CommitFlushInterval = 0
	e := New(cfg, src, sink, metrics.New(), nil)
	runUntil(t, e, sink, 130, 5*time.Second)

	rounds := sink.committedRounds()
	for i := 1; i < len(rounds); i++ {
		if rounds[i] != rounds[i-1]+1 {
			t.Fatalf("order broken: %v", rounds)
		}
	}
	sizes := sink.recordedBatchSizes()
	sawLarge := false
	for _, s := range sizes {
		if s > 1 {
			sawLarge = true
			break
		}
	}
	if !sawLarge {
		t.Fatalf("expected batched commits, sizes=%v", sizes)
	}
}

func TestBatchFailureDoesNotAdvanceCheckpoint(t *testing.T) {
	src := newFakeSource(100, 120)
	sink := &mockSink{}
	sink.failOnce.Store(true)
	cfg := testCfg(4, 32)
	cfg.Sync.StartRound = "100"
	cfg.Sync.CommitBatchSize = 10
	e := New(cfg, src, sink, metrics.New(), nil)
	runUntil(t, e, sink, 120, 5*time.Second)

	rounds := uniqueSorted(sink.committedRounds())
	if rounds[0] != 100 {
		t.Fatalf("expected restart from 100 after failed batch, got %v", rounds)
	}
	for i := 1; i < len(rounds); i++ {
		if rounds[i] != rounds[i-1]+1 {
			t.Fatalf("gap after failed batch: %v", rounds)
		}
	}
}

func TestBoundedBatchingBackpressure(t *testing.T) {
	src := newFakeSource(300, 400)
	src.delay[300] = 120 * time.Millisecond
	sink := &mockSink{}
	const window = 8
	cfg := testCfg(4, window)
	cfg.Sync.StartRound = "300"
	cfg.Sync.CommitBatchSize = 50
	e := New(cfg, src, sink, metrics.New(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.Run(ctx) }()

	time.Sleep(60 * time.Millisecond)
	calls := src.getCalls.Load()
	if calls > int64(window*2) {
		cancel()
		<-done
		t.Fatalf("getCalls=%d exceeds window=%d with large batch size", calls, window)
	}
	cancel()
	<-done
}

func TestSleepRespectsCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err := sleep(ctx, time.Second)
	if err == nil {
		t.Fatal("expected cancel")
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatal("slept too long")
	}
}

func uniqueSorted(in []uint64) []uint64 {
	seen := map[uint64]struct{}{}
	var out []uint64
	for _, r := range in {
		if _, ok := seen[r]; ok {
			continue
		}
		seen[r] = struct{}{}
		out = append(out, r)
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}
