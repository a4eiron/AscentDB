package sstable

import (
	"container/list"
	"sync"
)

type BlockCache struct {
	mu  sync.Mutex
	lru *LRU
}

func NewBlockCache(maxBlocks int) *BlockCache {
	return &BlockCache{lru: newLRU(maxBlocks)}
}

func (bc *BlockCache) Get(fileNum, offset uint64) ([]byte, bool) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return bc.lru.get(fileNum, offset)
}

func (bc *BlockCache) Set(fileNum, offset uint64, data []byte) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.lru.set(fileNum, offset, data)
}

type entry struct {
	key   uint64
	value []byte
}

type LRU struct {
	cap   int
	ll    *list.List
	items map[uint64]*list.Element
}

func newLRU(cap int) *LRU {
	return &LRU{
		cap:   cap,
		ll:    list.New(),
		items: make(map[uint64]*list.Element),
	}
}

func packKey(fileNum, offset uint64) uint64 {
	return fileNum<<32 | (offset >> 3)
}

func (c *LRU) get(fileNum, offset uint64) ([]byte, bool) {
	k := packKey(fileNum, offset)
	ele, ok := c.items[k]
	if !ok {
		return nil, false
	}

	c.ll.MoveToFront(ele)
	return ele.Value.(*entry).value, true
}

func (c *LRU) set(fileNum, offset uint64, data []byte) {
	k := packKey(fileNum, offset)
	if ele, ok := c.items[k]; ok {
		c.ll.MoveToFront(ele)
		ele.Value.(*entry).value = data
		return
	}
	ele := c.ll.PushFront(&entry{key: k, value: data})
	c.items[k] = ele
	if c.ll.Len() > c.cap {
		c.evict()
	}
}

func (c *LRU) evict() {
	ele := c.ll.Back()
	if ele == nil {
		return
	}
	c.ll.Remove(ele)
	delete(c.items, uint64(ele.Value.(*entry).key))
}
