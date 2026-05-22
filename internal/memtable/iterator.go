package memtable

import (
	"github.com/a4eiron/ascentdb/internal/record"
)

type MemtableIterator struct {
	list *Skiplist
	curr *SkiplistNode
}

func (sl *Skiplist) Iterator() *MemtableIterator {
	return &MemtableIterator{
		list: sl,
		curr: sl.head.forward[0],
	}
}

func (iter *MemtableIterator) Next() {
	if iter.curr == nil {
		return
	}
	iter.curr = iter.curr.forward[0]
}

func (iter *MemtableIterator) Valid() bool {
	return iter.curr != nil
}

func (iter *MemtableIterator) Key() *record.InternalKey {
	if iter.curr != nil {
		return &iter.curr.key
	}
	return &record.InternalKey{}
}

func (iter *MemtableIterator) Value() []byte {
	if iter.curr != nil {
		return iter.curr.value
	}
	return nil
}

func (iter *MemtableIterator) Seek(target record.InternalKey) {
	curr := iter.list.head
	for i := iter.list.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && iter.list.compare(curr.forward[i].key, target) < 0 {
			curr = curr.forward[i]
		}
	}
	iter.curr = curr.forward[0]
}

func (iter *MemtableIterator) Rewind() {
	iter.curr = iter.list.head.forward[0]
}
