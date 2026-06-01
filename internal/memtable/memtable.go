package memtable

import (
	"sync"

	"github.com/a4eiron/ascentdb/internal/record"
)

type Memtable struct {
	list *Skiplist

	size    uint64
	maxSize uint64

	mu sync.RWMutex
}

func New(maxSize uint64, slMaxLevel uint, slP float64) *Memtable {
	compareFn := func(a, b record.InternalKey) int { return a.Compare(b) }

	return &Memtable{
		list:    NewSkiplist(slMaxLevel, slP, compareFn),
		maxSize: maxSize,
	}
}

func (m *Memtable) Put(r record.Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.list.insert(r.InternalKey, r.Value)
	m.size += uint64(r.Size())
	return nil
}

func (m *Memtable) Get(userKey []byte, lookupKey record.InternalKey) ([]byte, bool, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.list.search(userKey, lookupKey)
}

func (m *Memtable) Size() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.size
}

func (m *Memtable) IsFull() bool {
	return m.size >= m.maxSize
}

func (m *Memtable) Iterator() *MemtableIterator {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.list.Iterator()
}
