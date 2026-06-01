package memtable

import (
	"sync"
	"sync/atomic"

	"github.com/a4eiron/ascentdb/internal/record"
)

type Memtable struct {
	list *Skiplist

	size    uint64
	maxSize uint64

	refs atomic.Int32

	mu sync.RWMutex
}

func New(maxSize uint64, slMaxLevel uint, slP float64) *Memtable {
	compareFn := func(a, b record.InternalKey) int { return a.Compare(b) }

	mt := &Memtable{
		list:    NewSkiplist(slMaxLevel, slP, compareFn),
		maxSize: maxSize,
	}
	mt.refs.Add(1)

	return mt
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

func (m *Memtable) Ref() {
	m.refs.Add(1)
}

func (m *Memtable) Unref() {
	m.refs.Add(-1)
}
