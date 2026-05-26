package sstable

import (
	"encoding/binary"

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

// [num_entries(4)][[entry_len(4)][entry]...]
func encodeIndexBlock(b *IndexBlock) []byte {
	encoded := make([][]byte, len(b.entries))
	total := 4
	for i, entry := range b.entries {
		encoded[i] = encodeIndexEntry(&entry)
		total += 4 + len(encoded[i])
	}

	buf := make([]byte, total)
	off := 0
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(b.entries)))
	off += 4

	for _, eb := range encoded {
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(eb)))
		off += 4
		copy(buf[off:], eb)
		off += len(eb)
	}
	return buf
}

func decodeIndexBlock(b []byte) (*IndexBlock, error) {
	if len(b) < 4 {
		return nil, record.ErrCorruptRecord
	}
	off := 0
	numEntries := int(binary.LittleEndian.Uint32(b[off:]))
	off += 4

	block := &IndexBlock{entries: make([]IndexEntry, 0, numEntries)}

	for range numEntries {
		if off+4 > len(b) {
			return nil, record.ErrCorruptRecord
		}
		entrySize := int(binary.LittleEndian.Uint32(b[off:]))
		off += 4

		if off+entrySize > len(b) {
			return nil, record.ErrCorruptRecord
		}
		entry, err := decodeIndexEntry(b[off : off+entrySize])
		if err != nil {
			return nil, err
		}
		block.entries = append(block.entries, entry)
		off += entrySize
	}

	return block, nil
}

// [block_offset(8)][block_size(4)][separator_key...]
func encodeIndexEntry(e *IndexEntry) []byte {
	keySize := int(e.SeparatorKey.KeySize())
	buf := make([]byte, 8+4+keySize)
	off := 0

	binary.LittleEndian.PutUint64(buf[off:], e.BlockOffset)
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], e.BlockSize)
	off += 4
	record.EncodeInternalKey(buf, off, e.SeparatorKey)

	return buf
}

func decodeIndexEntry(b []byte) (IndexEntry, error) {
	if len(b) < 12 {
		return IndexEntry{}, record.ErrCorruptRecord
	}
	off := 0

	blockOffset := binary.LittleEndian.Uint64(b[off:])
	off += 8
	blockSize := binary.LittleEndian.Uint32(b[off:])
	off += 4

	sepKey, _, err := record.DecodeInternalKey(b, off)
	if err != nil {
		return IndexEntry{}, err
	}

	return IndexEntry{
		BlockOffset:  blockOffset,
		BlockSize:    blockSize,
		SeparatorKey: sepKey,
	}, nil
}
