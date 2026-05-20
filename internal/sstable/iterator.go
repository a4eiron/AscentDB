package sstable

import (
	"github.com/a4eiron/ascentdb/internal/record"
)

type Iterator struct {
	reader     *TableReader
	blockIndex int
	block      *Block
	entryIndex int
}

func (r *TableReader) Iterator() *Iterator {
	iter := &Iterator{
		reader:     r,
		blockIndex: 0,
	}
	iter.loadBlock()
	return iter
}

func (iter *Iterator) Valid() bool {
	return iter.block != nil && iter.entryIndex < len(iter.block.entries)
}

func (iter *Iterator) Next() {
	if iter.entryIndex+1 < len(iter.block.entries) {
		iter.entryIndex++
		return
	}

	iter.blockIndex++
	iter.loadBlock()
}

func (iter *Iterator) Key() *record.InternalKey {
	return iter.block.entries[iter.entryIndex].InternalKey
}

func (iter *Iterator) Value() []byte {
	return iter.block.entries[iter.entryIndex].Value
}

func (iter *Iterator) loadBlock() {
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
