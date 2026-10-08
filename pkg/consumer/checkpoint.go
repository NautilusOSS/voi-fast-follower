package consumer

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// CheckpointFile stores a consumer's durable cursor (independent from follower checkpoint).
type CheckpointFile struct {
	Path string
}

// Load returns the last fully processed round, if any.
func (c CheckpointFile) Load() (round uint64, ok bool, err error) {
	if strings.TrimSpace(c.Path) == "" {
		return 0, false, nil
	}
	b, err := os.ReadFile(c.Path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, err
	}
	s := strings.TrimSpace(string(b))
	if s == "" {
		return 0, false, nil
	}
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse consumer checkpoint: %w", err)
	}
	return n, true, nil
}

// Save records the last fully processed round (atomic write).
func (c CheckpointFile) Save(round uint64) error {
	if strings.TrimSpace(c.Path) == "" {
		return nil
	}
	tmp := c.Path + ".tmp"
	content := []byte(strconv.FormatUint(round, 10) + "\n")
	if err := os.WriteFile(tmp, content, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.Path)
}

// ResumeRound returns startRound unless checkpoint exists, then checkpoint+1.
func (c CheckpointFile) ResumeRound(startRound uint64) (uint64, error) {
	cp, ok, err := c.Load()
	if err != nil || !ok {
		return startRound, err
	}
	return cp + 1, nil
}
