package consumer

import (
	"errors"
	"fmt"
)

var (
	// ErrNotYetAvailable means the round is not durable yet (archive growing or live tip pending).
	ErrNotYetAvailable = errors.New("consumer: round not yet available")

	// ErrNotFound means the round is not in the archive and will not appear (pruned or never captured).
	ErrNotFound = errors.New("consumer: round not found in archive")

	// ErrGap means a contiguous round was missing from the source (integrity failure).
	ErrGap = errors.New("consumer: gap in block stream")

	// ErrCorrupt means archive or block data failed validation.
	ErrCorrupt = errors.New("consumer: corrupt block data")
)

// RoundError annotates an error with the requested round.
type RoundError struct {
	Round uint64
	Err   error
}

func (e *RoundError) Error() string {
	return fmt.Sprintf("round %d: %v", e.Round, e.Err)
}

func (e *RoundError) Unwrap() error { return e.Err }

func roundErr(round uint64, err error) error {
	if err == nil {
		return nil
	}
	return &RoundError{Round: round, Err: err}
}
