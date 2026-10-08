package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// segRef locates a segment file by its header/filename start round.
type segRef struct {
	start uint64
	path  string
}

// rebuildIndexLocked scans the segments directory and builds a sorted index.
// Callers must hold s.mu (or invoke during construction before concurrent use).
func (s *ArchiveSink) rebuildIndexLocked() error {
	entries, err := os.ReadDir(s.segmentsDir())
	if err != nil {
		return err
	}
	var refs []segRef
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		start, err := parseSegmentStartName(e.Name())
		if err != nil {
			// Fall back to header.
			path := filepath.Join(s.segmentsDir(), e.Name())
			f, err := os.Open(path)
			if err != nil {
				continue
			}
			start, err = readSegmentHeader(f)
			_ = f.Close()
			if err != nil {
				continue
			}
		}
		refs = append(refs, segRef{
			start: start,
			path:  filepath.Join(s.segmentsDir(), e.Name()),
		})
	}
	sort.Slice(refs, func(i, j int) bool { return refs[i].start < refs[j].start })
	s.segIndex = refs
	return nil
}

func parseSegmentStartName(name string) (uint64, error) {
	base := strings.TrimSuffix(name, ".seg")
	if len(base) == 0 {
		return 0, fmt.Errorf("empty segment name")
	}
	return strconv.ParseUint(base, 10, 64)
}

// segmentPathForRoundLocked returns the segment file that should contain round,
// using the in-memory index (binary search on start rounds).
func (s *ArchiveSink) segmentPathForRoundLocked(round uint64) (string, bool) {
	if len(s.segIndex) == 0 {
		return "", false
	}
	// Largest start ≤ round.
	i := sort.Search(len(s.segIndex), func(i int) bool {
		return s.segIndex[i].start > round
	}) - 1
	if i < 0 {
		return "", false
	}
	return s.segIndex[i].path, true
}
