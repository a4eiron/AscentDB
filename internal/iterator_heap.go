package internal

import (
	"container/heap"

	"github.com/a4eiron/ascentdb/internal/record"
)

type Item struct {
	Iter   Iterator
	Record *record.Record
}

type IteratorHeap struct {
	items []*Item
}

func (h IteratorHeap) Len() int {
	return len(h.items)
}

func (h IteratorHeap) Less(i, j int) bool {
	return h.items[i].Record.Compare(*h.items[j].Record.InternalKey) < 0
}

func (h IteratorHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *IteratorHeap) Push(x any) {
	h.items = append(h.items, x.(*Item))
}

func (h *IteratorHeap) Pop() any {
	old := h.items
	n := len(old)

	item := old[n-1]
	h.items = old[:n-1]

	return item
}

func NewIteratorHeap(iters []Iterator) *IteratorHeap {
	h := &IteratorHeap{}
	heap.Init(h)

	for _, iter := range iters {
		if !iter.Valid() {
			continue
		}
		heap.Push(h, &Item{
			Iter: iter,
			Record: &record.Record{
				InternalKey: iter.Key(),
				Value:       iter.Value(),
			},
		})
	}
	return h
}

func (h *IteratorHeap) Empty() bool {
	return h.Len() == 0
}

func (h *IteratorHeap) Peek() *Item {
	if h.Len() == 0 {
		return nil
	}
	return h.items[0]
}

func (h *IteratorHeap) PopAndAdvance() *Item {
	if h.Len() == 0 {
		return nil
	}
	item := heap.Pop(h).(*Item)

	key := *item.Record.InternalKey
	val := item.Record.Value
	result := &Item{
		Record: &record.Record{
			InternalKey: &key,
			Value:       val,
		},
	}

	item.Iter.Next()

	if item.Iter.Valid() {
		item.Record.InternalKey = item.Iter.Key()
		item.Record.Value = item.Iter.Value()
		heap.Push(h, item)
	}

	return result
}
