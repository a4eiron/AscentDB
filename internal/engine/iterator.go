package engine

import (
	"bytes"

	"github.com/a4eiron/ascentdb/internal"
	"github.com/a4eiron/ascentdb/internal/record"
)

type ScanIterator struct {
	iters      []internal.Iterator
	end        []byte
	seqNum     uint64
	currentRec record.Record
	current    *record.Record
	heap       *internal.IteratorHeap
}

func NewScanIterator(iters []internal.Iterator, end []byte, seqNum uint64) *ScanIterator {
	s := &ScanIterator{
		iters:  iters,
		end:    end,
		seqNum: seqNum,
		heap:   internal.NewIteratorHeap(iters),
	}
	s.advance()
	return s
}

func (sIter *ScanIterator) Valid() bool {
	return sIter.current != nil
}

func (sIter *ScanIterator) Key() record.InternalKey {
	return sIter.current.InternalKey
}

func (sIter *ScanIterator) Value() []byte {
	return sIter.current.Value
}

func (sIter *ScanIterator) Next() {
	if sIter.current == nil {
		return
	}

	sIter.advance()
}

func (s *ScanIterator) Seek(target record.InternalKey) {
	for _, it := range s.iters {
		it.Seek(target)
	}
	s.heap = internal.NewIteratorHeap(s.iters)
	s.advance()
}

func (sIter *ScanIterator) advance() {

	for !sIter.heap.Empty() {
		userKey := sIter.heap.Peek().Record.UserKey

		if bytes.Compare(userKey, sIter.end) > 0 {
			sIter.current = nil
			return
		}
		var best record.Record
		found := false

		for !sIter.heap.Empty() && bytes.Equal(sIter.heap.Peek().Record.UserKey, userKey) {
			rec := sIter.heap.PopAndAdvance()

			if rec.SeqNum > sIter.seqNum {
				continue
			}

			if !found || rec.SeqNum > best.SeqNum {
				best = rec
				found = true
			}
		}

		if !found || best.IsTombstone() {
			continue
		}

		sIter.currentRec = best
		sIter.current = &sIter.currentRec
		return
	}
	sIter.current = nil
}
