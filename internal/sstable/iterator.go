package sstable

import (
	"github.com/a4eiron/ascentdb/internal/record"
)

type SSTableIterator struct {
	reader     *TableReader
	blockIndex int
	block      *Block
	entryIndex int
}

func (r *TableReader) Iterator() *SSTableIterator {
	iter := &SSTableIterator{
		reader:     r,
		blockIndex: 0,
	}
	iter.loadBlock()
	return iter
}

func (iter *SSTableIterator) Valid() bool {
	return iter.block != nil && iter.entryIndex < len(iter.block.entries)
}

func (iter *SSTableIterator) Next() {
	if iter.entryIndex+1 < len(iter.block.entries) {
		iter.entryIndex++
		return
	}

	iter.blockIndex++
	iter.loadBlock()
}

func (iter *SSTableIterator) Key() record.InternalKey {
	return iter.block.entries[iter.entryIndex].InternalKey
}

func (iter *SSTableIterator) Value() []byte {
	return iter.block.entries[iter.entryIndex].Value
}

func (iter *SSTableIterator) Seek(target record.InternalKey) {
	idx := iter.reader.findBlockIndex(target)
	if idx >= len(iter.reader.index.entries) {
		iter.block = nil
		return
	}
	iter.blockIndex = idx
	iter.loadBlock()
	for iter.entryIndex < len(iter.block.entries) && iter.Valid() && iter.Key().Compare(target) < 0 {
		iter.entryIndex++
	}

	if iter.entryIndex >= len(iter.block.entries) {
		iter.blockIndex++
		iter.loadBlock()
	}
}

func (iter *SSTableIterator) loadBlock() {
	if iter.blockIndex >= len(iter.reader.index.entries) {
		iter.block = nil
		return
	}

	entry := &iter.reader.index.entries[iter.blockIndex]

	block, err := iter.reader.readBlock(entry.BlockOffset, entry.BlockSize)
	if err != nil {
		iter.block = nil
		return
	}

	iter.block = block
	iter.entryIndex = 0
}
