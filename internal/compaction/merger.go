package compaction

import (
	"container/heap"

	"github.com/a4eiron/ascentdb/internal/record"
	"github.com/a4eiron/ascentdb/internal/sstable"
)

type MergeIterator struct {
	heap minHeap
}

type heapItem struct {
	iter   *sstable.SSTableIterator
	record *record.Record
}

type minHeap []*heapItem

func (h minHeap) Len() int { return len(h) }

func (h minHeap) Less(i, j int) bool {
	cmp := h[i].record.Compare(*h[j].record.InternalKey)
	return cmp < 0
}

func (h minHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *minHeap) Push(x any) {
	item := x.(*heapItem)
	*h = append(*h, item)
}

func (h *minHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func NewMergeIterator(iters []*sstable.SSTableIterator) *MergeIterator {
	h := &minHeap{}
	heap.Init(h)

	for _, iter := range iters {
		if !iter.Valid() {
			continue
		}
		heap.Push(h, &heapItem{
			iter: iter,
			record: &record.Record{
				InternalKey: iter.Key(),
				Value:       iter.Value(),
			},
		})
	}

	return &MergeIterator{heap: *h}

}

func (iter *MergeIterator) Valid() bool {
	return len(iter.heap) > 0
}

func (iter *MergeIterator) Next() {
	item := heap.Pop(&iter.heap).(*heapItem)
	item.iter.Next()
	if item.iter.Valid() {
		item.record = &record.Record{
			InternalKey: item.iter.Key(),
			Value:       item.iter.Value(),
		}
		heap.Push(&iter.heap, item)
	}
}

func (iter *MergeIterator) Record() *record.Record {
	return iter.heap[0].record
}
