// Package consumer provides an experimental public API for reading the canonical
// Voi block stream from a Fast Follower archive (and optionally live algod).
//
// # Stability
//
// This package is **experimental** (v0). Types and function signatures may change
// until promoted to stable in a future release. Prefer pinning a git tag for
// production consumers.
//
// Stable concepts (documented, not necessarily frozen as Go symbols):
//
//   - Block — canonical raw block with msgpack payload preserved
//   - Source — ordered block access by round
//   - consumer checkpoint — independent from follower checkpoint
//   - archive — durable historical store
//
// Internal follower packages (internal/follower, fetch workers, ordered buffer)
// are not part of this API.
//
// # Ordering and delivery
//
// Blocks are read in strict round order when using Cursor or Process.
// Delivery is at-least-once: after a consumer crash, restart from
// consumer_checkpoint + 1 and tolerate duplicate rounds.
//
// # Raw bytes
//
// Block.Raw contains a copy of the algod BlockRaw msgpack bytes. Callers own
// the returned slice; do not mutate shared buffers.
//
// # Follower vs consumer checkpoints
//
// The follower checkpoint (archive/checkpoint file) is independent from a
// consumer's own cursor file. A consumer may lag behind the follower without
// affecting acquisition. The follower never rewinds for a slow consumer.
package consumer
