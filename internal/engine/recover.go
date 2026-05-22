package engine

import (
	"sync/atomic"

	"github.com/a4eiron/ascentdb/internal/record"
	"github.com/a4eiron/ascentdb/internal/wal"
)

func (e *Engine) recover() error {
	var maxSeq uint64
	err := wal.Replay(e.wal, func(r *record.Record) error {
		e.mt.Put(r)
		if r.SeqNum > maxSeq {
			maxSeq = r.SeqNum
		}
		return nil
	})

	if err != nil {
		return err
	}

	if maxSeq > e.seqNum {
		atomic.StoreUint64(&e.seqNum, maxSeq)
	}
	return nil
}
