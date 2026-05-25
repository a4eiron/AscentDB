package sstable

import (
	"github.com/a4eiron/ascentdb/internal/codec"
	"github.com/a4eiron/ascentdb/internal/record"
)

type IndexBlock struct {
	entries []IndexEntry
}

type IndexEntry struct {
	SeparatorKey record.InternalKey
	BlockOffset  uint64
	BlockSize    uint32
}

// [num_entries(4)]
// [entries(variant)...]
func encodeIndexBlock(b *IndexBlock) []byte {
	var size int

	size += 4 // num_entries

	encodedEntries := make([][]byte, 0, len(b.entries))

	for _, entry := range b.entries {
		encodedEntry := encodeIndexEntry(&entry)
		encodedEntries = append(encodedEntries, encodedEntry)
		size += 4
		size += len(encodedEntry)
	}

	buf := codec.NewBuffer(size)

	buf.WriteUint32(uint32(len(b.entries)))
	for _, eb := range encodedEntries {
		buf.WriteUint32(uint32(len(eb)))
		buf.WriteBytes(eb)
	}

	return buf.Bytes()
}

func decodeIndexBlock(b []byte) (*IndexBlock, error) {
	buf := codec.NewBufferFromBytes(b)
	numEntries, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	block := &IndexBlock{entries: make([]IndexEntry, 0, numEntries)}

	for range numEntries {
		entrySize, err := buf.ReadUint32()
		if err != nil {
			return nil, err
		}

		entryBytes, err := buf.ReadBytes(int(entrySize))
		if err != nil {
			return nil, err
		}

		entry, err := decodeIndexEntry(entryBytes)
		if err != nil {
			return nil, err
		}

		block.entries = append(block.entries, *entry)
	}

	return block, nil
}

func encodeIndexEntry(e *IndexEntry) []byte {
	size := int(e.SeparatorKey.KeySize()) + 8 + 4

	buf := codec.NewBuffer(size)

	buf.WriteUint64(e.BlockOffset)
	buf.WriteUint32(e.BlockSize)
	record.EncodeInternalKey(buf, e.SeparatorKey)

	return buf.Bytes()
}

func decodeIndexEntry(b []byte) (*IndexEntry, error) {

	buf := codec.NewBufferFromBytes(b)

	blockOffset, err := buf.ReadUint64()
	if err != nil {
		return nil, err
	}

	blockSize, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	separatorKey, err := record.DecodeInternalKey(buf)
	if err != nil {
		return nil, err
	}

	entry := &IndexEntry{
		BlockOffset:  blockOffset,
		BlockSize:    blockSize,
		SeparatorKey: separatorKey,
	}

	return entry, nil

}
