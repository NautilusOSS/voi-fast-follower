package follower

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/config"
	"github.com/nicholasshellabarger/voi-fast-follower/internal/metrics"
)

type mockSink struct {
	mu      sync.Mutex
	rounds  []uint64
	last    uint64
	hasLast bool
	hashes  map[uint64]string
}

func (m *mockSink) ProcessBlock(_ context.Context, blk block.Block) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rounds = append(m.rounds, blk.Round)
	m.last = blk.Round
	m.hasLast = true
	if m.hashes == nil {
		m.hashes = map[uint64]string{}
	}
	m.hashes[blk.Round] = blk.BlockHash
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

func TestResolveStartRoundPrefersCheckpoint(t *testing.T) {
	cfg := &config.Config{
		Sync: config.SyncConfig{StartRound: "100"},
	}
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

func TestCommitChainBreak(t *testing.T) {
	sink := &mockSink{
		hashes: map[uint64]string{10: "AAAA"},
	}
	e := New(&config.Config{}, nil, sink, metrics.New(), nil)
	err := e.commit(context.Background(), block.Block{
		Round:             11,
		PreviousBlockHash: "BBBB",
		BlockHash:         "CCCC",
	})
	if err == nil {
		t.Fatal("expected chain break error")
	}
}

func TestCommitIdempotentPath(t *testing.T) {
	sink := &mockSink{}
	e := New(&config.Config{}, nil, sink, metrics.New(), nil)
	blk := block.Block{Round: 1, BlockHash: "H1", PreviousBlockHash: ""}
	if err := e.commit(context.Background(), blk); err != nil {
		t.Fatal(err)
	}
	if err := e.commit(context.Background(), blk); err != nil {
		t.Fatal(err)
	}
	if len(sink.rounds) != 2 {
		t.Fatalf("expected 2 process calls, got %d", len(sink.rounds))
	}
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
