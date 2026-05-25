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

func (bc *BlockCache) Get(fileNum, offset uint64) (*Block, bool) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	return bc.lru.get(fileNum, offset)
}

func (bc *BlockCache) Set(fileNum, offset uint64, data *Block) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.lru.set(fileNum, offset, data)
}

type entry struct {
	key   blockKey
	value *Block
}

type blockKey struct {
	fileNum uint64
	offset  uint64
}

type LRU struct {
	cap   int
	ll    *list.List
	items map[blockKey]*list.Element
}

func newLRU(cap int) *LRU {
	return &LRU{
		cap:   cap,
		ll:    list.New(),
		items: make(map[blockKey]*list.Element),
	}
}

func (c *LRU) get(fileNum, offset uint64) (*Block, bool) {
	ele, ok := c.items[blockKey{fileNum: fileNum, offset: offset}]
	if !ok {
		return nil, false
	}

	c.ll.MoveToFront(ele)
	return ele.Value.(*entry).value, true
}

func (c *LRU) set(fileNum, offset uint64, data *Block) {
	key := blockKey{fileNum: fileNum, offset: offset}
	if ele, ok := c.items[key]; ok {
		c.ll.MoveToFront(ele)
		ele.Value.(*entry).value = data
		return
	}
	ele := c.ll.PushFront(&entry{key: key, value: data})
	c.items[key] = ele
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
	delete(c.items, ele.Value.(*entry).key)
}
