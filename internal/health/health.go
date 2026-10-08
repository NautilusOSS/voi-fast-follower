package health

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Mode describes operational state for readiness probes.
type Mode string

const (
	ModeStartup    Mode = "startup"
	ModeCatchingUp Mode = "catching_up"
	ModeLive       Mode = "live"
	ModeDegraded   Mode = "degraded"
	ModeFailed     Mode = "failed"
)

// Snapshot is a point-in-time health view.
type Snapshot struct {
	Mode        Mode      `json:"mode"`
	Tip         uint64    `json:"tip_round"`
	NextCommit  uint64    `json:"next_commit_round"`
	Checkpoint  uint64    `json:"checkpoint_round"`
	HasCP       bool      `json:"has_checkpoint"`
	Lag         uint64    `json:"lag"`
	SinkError   string    `json:"sink_error,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
	LiveNearTip uint64    `json:"-"` // lag threshold for "live" (default 2)
}

// Tracker is a concurrency-safe health publisher.
type Tracker struct {
	mu   sync.RWMutex
	snap Snapshot
}

// New creates a startup-mode tracker.
func New() *Tracker {
	return &Tracker{snap: Snapshot{Mode: ModeStartup, LiveNearTip: 2, UpdatedAt: time.Now()}}
}

// SetFailed marks the process unable to continue.
func (t *Tracker) SetFailed(err string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.snap.Mode = ModeFailed
	t.snap.LastError = err
	t.snap.UpdatedAt = time.Now()
}

// SetSinkError records a sink failure (degraded until cleared by progress).
func (t *Tracker) SetSinkError(err string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.snap.SinkError = err
	if t.snap.Mode != ModeFailed && t.snap.Mode != ModeStartup {
		t.snap.Mode = ModeDegraded
	}
	t.snap.UpdatedAt = time.Now()
}

// ClearSinkError clears degraded sink state.
func (t *Tracker) ClearSinkError() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.snap.SinkError = ""
	t.snap.UpdatedAt = time.Now()
}

// UpdateProgress refreshes tip/checkpoint and derives mode.
func (t *Tracker) UpdateProgress(nextCommit, tip, checkpoint uint64, hasCP bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.snap.NextCommit = nextCommit
	t.snap.Tip = tip
	t.snap.Checkpoint = checkpoint
	t.snap.HasCP = hasCP
	cursor := checkpoint
	if !hasCP && nextCommit > 0 {
		cursor = nextCommit - 1
	}
	if tip > cursor {
		t.snap.Lag = tip - cursor
	} else {
		t.snap.Lag = 0
	}
	near := t.snap.LiveNearTip
	if near == 0 {
		near = 2
	}
	switch {
	case t.snap.Mode == ModeFailed:
		// keep failed
	case t.snap.SinkError != "":
		t.snap.Mode = ModeDegraded
	case !hasCP && nextCommit == 0:
		t.snap.Mode = ModeStartup
	case t.snap.Lag > near:
		t.snap.Mode = ModeCatchingUp
	default:
		t.snap.Mode = ModeLive
	}
	t.snap.UpdatedAt = time.Now()
}

// Snapshot returns a copy of the current health state.
func (t *Tracker) Snapshot() Snapshot {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.snap
}

// Ready reports whether the process is fit to receive traffic (not startup/failed).
func (t *Tracker) Ready() bool {
	s := t.Snapshot()
	return s.Mode == ModeLive || s.Mode == ModeCatchingUp || s.Mode == ModeDegraded
}

// Live reports whether the follower is near tip without sink errors.
func (t *Tracker) Live() bool {
	s := t.Snapshot()
	return s.Mode == ModeLive
}

// RegisterHandlers mounts /healthz and /readyz.
func (t *Tracker) RegisterHandlers(mux *http.ServeMux) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		s := t.Snapshot()
		if s.Mode == ModeFailed {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		_ = json.NewEncoder(w).Encode(s)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		s := t.Snapshot()
		if !t.Ready() {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		_ = json.NewEncoder(w).Encode(s)
	})
}
