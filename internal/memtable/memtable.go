package memtable

import (
	"sync"

	"github.com/a4eiron/ascentdb/internal/record"
)

type Memtable struct {
	list *Skiplist

	size    uint64
	maxSize uint64

	mu sync.Mutex
}

func New(maxSize uint64) *Memtable {
	compareFn := func(a, b record.InternalKey) int { return a.Compare(b) }

	return &Memtable{
		list:    NewSkiplist(16, compareFn),
		maxSize: maxSize,
	}
}

func (m *Memtable) Put(r *record.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.list.insert(*r.InternalKey, r.Value)
	m.size += uint64(r.Size())
	return nil
}

func (m *Memtable) Get(userKey string) ([]byte, bool, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.list.search(userKey)
}

func (m *Memtable) Size() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.size
}

func (m *Memtable) IsFull() bool {
	return m.size >= m.maxSize
}

// func (m *Memtable) PrintAll() {
// 	iter := m.list.Iterator()
// 	for iter.Valid() {
// 		k := iter.Key()
// 		v := iter.Value()
// 		fmt.Printf("Key: %s, Seq: %d, Val: %s\n", k.UserKey, k.SeqNum, string(v))
// 		iter.Next()
// 	}
// }

func (m *Memtable) Iterator() *Iterator {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.list.Iterator()
}
