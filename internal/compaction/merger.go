package compaction

import (
	"github.com/a4eiron/ascentdb/internal"
	"github.com/a4eiron/ascentdb/internal/record"
	"github.com/a4eiron/ascentdb/internal/sstable"
)

type MergeIterator struct {
	heap *internal.IteratorHeap
}

func NewMergeIterator(iters []*sstable.SSTableIterator) *MergeIterator {
	heapIters := make([]internal.Iterator, len(iters))
	for i := range iters {
		heapIters[i] = iters[i]
	}

	return &MergeIterator{heap: internal.NewIteratorHeap(heapIters)}
}

func (iter *MergeIterator) Valid() bool {
	return !iter.heap.Empty()

}

func (iter *MergeIterator) Next() {
	iter.heap.PopAndAdvance()
}

func (iter *MergeIterator) Record() *record.Record {
	return iter.heap.Peek().Record
}
