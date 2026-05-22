package memtable

import (
	"math/rand"

	"github.com/a4eiron/ascentdb/internal/record"
)

const P = 0.25

type SkiplistNode struct {
	key     record.InternalKey
	value   []byte
	forward []*SkiplistNode
}

type Skiplist struct {
	head     *SkiplistNode
	level    int
	maxLevel int
	compare  func(a, b record.InternalKey) int
}

func NewSkiplist(maxLevel uint, compare func(a, b record.InternalKey) int) *Skiplist {
	return &Skiplist{
		head: &SkiplistNode{
			forward: make([]*SkiplistNode, maxLevel),
		},
		level:    1,
		maxLevel: int(maxLevel),
		compare:  compare,
	}
}

func (sl *Skiplist) insert(key record.InternalKey, value []byte) {
	update := make([]*SkiplistNode, sl.maxLevel)
	curr := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && sl.compare(curr.forward[i].key, key) < 0 {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	lvl := sl.randomLevel()
	if lvl > sl.level {
		for i := sl.level; i < lvl; i++ {
			update[i] = sl.head
		}
		sl.level = lvl
	}

	newNode := &SkiplistNode{
		key:     key,
		value:   value,
		forward: make([]*SkiplistNode, lvl),
	}

	for i := range lvl {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

}

func (sl *Skiplist) search(userKey string, lookupKey record.InternalKey) ([]byte, bool, bool) {
	curr := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil && sl.compare(curr.forward[i].key, lookupKey) < 0 {
			curr = curr.forward[i]
		}
	}

	curr = curr.forward[0]

	if curr != nil && curr.key.UserKey == userKey {
		if curr.key.Type == record.TypeDel {
			return nil, true, true
		}
		return curr.value, true, false
	}
	return nil, false, false
}

func (sl *Skiplist) randomLevel() int {
	lvl := 1
	for rand.Float64() < P && lvl < sl.maxLevel {
		lvl++
	}
	return lvl
}
