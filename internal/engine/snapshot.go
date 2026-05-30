package engine

import (
	"math"
	"slices"
	"sync"
	"sync/atomic"
)

type Snapshot struct {
	seqNum uint64
	engine *Engine
}

type SnapshotList struct {
	mu        sync.Mutex
	snapshots []uint64
}

// creates a snapshot of the current engine state
// caller need to call snapshot.Release() when done to avoid leaks
func (e *Engine) NewSnapshot() *Snapshot {
	seq := atomic.LoadUint64(&e.seqNum)
	e.snapshots.add(seq)
	return &Snapshot{
		seqNum: seq,
		engine: e,
	}
}

func (s *Snapshot) Get(key []byte) ([]byte, bool) {
	return s.engine.get(key, s.seqNum)
}

func (s *Snapshot) Scan(start, end []byte) *ScanIterator {
	return s.engine.scan(start, end, s.seqNum)
}

func (s *Snapshot) Release() {
	s.engine.snapshots.remove(s.seqNum)
}

func (sl *SnapshotList) add(seq uint64) {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	sl.snapshots = append(sl.snapshots, seq)
	slices.Sort(sl.snapshots)
}

func (sl *SnapshotList) remove(seq uint64) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	for i, s := range sl.snapshots {
		if s == seq {
			sl.snapshots = append(sl.snapshots[:i], sl.snapshots[i+1:]...)
			return
		}
	}
}

func (sl *SnapshotList) oldest() uint64 {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if len(sl.snapshots) == 0 {
		return math.MaxUint64
	}
	return sl.snapshots[0]
}
