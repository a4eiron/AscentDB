package engine

import (
	"github.com/a4eiron/ascentdb/internal"
	"github.com/a4eiron/ascentdb/internal/record"
)

type ScanIterator struct {
	iters      []internal.Iterator
	end        string
	seqNum     uint64
	currentKey record.InternalKey
	currentRec record.Record
	current    *record.Record
	heap       *internal.IteratorHeap
}

func NewScanIterator(iters []internal.Iterator, end string, seqNum uint64) *ScanIterator {
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

func (sIter *ScanIterator) Key() *record.InternalKey {
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

		if userKey > sIter.end {
			sIter.current = nil
			return
		}
		var best *record.Record
		for !sIter.heap.Empty() && sIter.heap.Peek().Record.UserKey == userKey {
			item := sIter.heap.PopAndAdvance()

			if item.Record.SeqNum > sIter.seqNum {
				continue
			}

			if best == nil || item.Record.SeqNum > best.SeqNum {
				best = item.Record
			}
		}

		if best == nil || best.IsTombstone() {
			continue
		}

		sIter.current = best
		return
	}
	sIter.current = nil
}
