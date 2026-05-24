package engine

import (
	"github.com/a4eiron/ascentdb/internal"
	"github.com/a4eiron/ascentdb/internal/record"
)

type ScanIterator struct {
	iters   []internal.Iterator
	end     string
	current *record.Record
	buf     record.Record
	bufKey  record.InternalKey
}

func NewScanIterator(iters []internal.Iterator, end string) *ScanIterator {
	s := &ScanIterator{
		iters: iters,
		end:   end,
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

	lastKey := sIter.current.UserKey
	for _, iter := range sIter.iters {
		for iter.Valid() && iter.Key().UserKey == lastKey {
			iter.Next()
		}
	}
	sIter.advance()
}

func (s *ScanIterator) Seek(target record.InternalKey) {
	for _, it := range s.iters {
		it.Seek(target)
	}
	s.advance()
}

func (sIter *ScanIterator) advance() {

	for {
		best := -1

		for i, iter := range sIter.iters {
			if !iter.Valid() {
				continue
			}

			if best == -1 {
				best = i
				continue
			}

			cmp := iter.Key().Compare(*sIter.iters[best].Key())
			if cmp < 0 {
				best = i
			} else if cmp == 0 && iter.Key().SeqNum > sIter.iters[best].Key().SeqNum {
				best = i
			}
		}

		if best == -1 {
			sIter.current = nil
			return
		}

		w := sIter.iters[best]
		key := w.Key()

		if key.UserKey > sIter.end {
			sIter.current = nil
			return
		}

		userKey := key.UserKey
		bestSeq := key.SeqNum
		bestType := key.Type
		bestVal := w.Value()

		for _, it := range sIter.iters {
			if !it.Valid() || it.Key().UserKey != userKey {
				continue
			}

			if it.Key().SeqNum > bestSeq {
				bestSeq = it.Key().SeqNum
				bestType = it.Key().Type
				bestVal = it.Value()
			}
		}

		for _, it := range sIter.iters {
			for it.Valid() && it.Key().UserKey == userKey {
				it.Next()
			}
		}

		if bestType == record.TypeDel {
			continue
		}

		sIter.bufKey = record.InternalKey{UserKey: userKey, SeqNum: bestSeq, Type: bestType}
		sIter.buf = record.Record{InternalKey: &sIter.bufKey, Value: bestVal}
		sIter.current = &sIter.buf
		// sIter.current = &record.Record{
		// 	InternalKey: &record.InternalKey{
		// 		UserKey: userKey,
		// 		SeqNum:  bestSeq,
		// 		Type:    bestType,
		// 	},
		// 	Value: bestVal,
		// }

		return
	}

}
