package sstable

import (
	"github.com/a4eiron/ascentdb/internal/codec"
	"github.com/a4eiron/ascentdb/internal/record"
)

type Block struct {
	entries []record.Record
}

func encodeBlock(b *Block) []byte {

	size := blockSize(*b)
	buf := codec.NewBuffer(size)

	// blocksize
	buf.WriteUint32(uint32(size))

	// num_entries
	buf.WriteUint32(uint32(len(b.entries)))

	// entries
	for _, entry := range b.entries {
		buf.WriteUint32(entry.Size())
		entryBytes := record.EncodeRecord(&entry)
		buf.WriteBytes(entryBytes)
	}

	return buf.Bytes()

}

func decodeBlock(b []byte) (*Block, error) {

	buf := codec.NewBufferFromBytes(b)
	_, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	numEntries, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	block := &Block{
		entries: make([]record.Record, 0),
	}

	for range numEntries {
		entrySize, err := buf.ReadUint32()
		if err != nil {
			return nil, err
		}

		entryBytes, err := buf.ReadBytes(int(entrySize))
		if err != nil {
			return nil, err
		}

		rec, err := record.DecodeRecord(entryBytes)
		if err != nil {
			return nil, err
		}

		block.entries = append(block.entries, *rec)
	}

	return block, nil
}

// [block_size(4)]
// [num_entries(4)]
// [[entry_size]entries(variant)...]
func blockSize(b Block) int {
	var size int
	size += 4 // for the entire blocksize
	size += 4 // for the no.of entries in the block
	size += len(b.entries) * 4

	for _, entry := range b.entries {
		size += int(entry.Size())
	}

	return size
}
