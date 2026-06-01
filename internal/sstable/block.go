package sstable

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/a4eiron/ascentdb/internal/record"
)

type Block struct {
	entries []record.Record
}

func decodeBlock(b []byte) (*Block, error) {
	if len(b) < 12 {
		return nil, record.ErrCorruptRecord
	}

	sumOffset := len(b) - 4
	expectedChecksum := binary.LittleEndian.Uint32(b[sumOffset:])
	actualChecksum := crc32.ChecksumIEEE(b[:sumOffset])

	if expectedChecksum != actualChecksum {
		return nil, record.ErrCorruptRecord
	}

	off := 0
	off += 4
	numEntries := int(binary.LittleEndian.Uint32(b[off:]))
	off += 4

	block := &Block{entries: make([]record.Record, numEntries)}

	for i := range numEntries {
		if off+4 > sumOffset {
			return nil, record.ErrCorruptRecord
		}
		entrySize := int(binary.LittleEndian.Uint32(b[off:]))
		off += 4

		if off+entrySize > len(b) {
			return nil, record.ErrCorruptRecord
		}
		rec, err := record.DecodeRecord(b[off : off+entrySize])
		if err != nil {
			return nil, err
		}
		block.entries[i] = rec
		off += entrySize
	}

	return block, nil
}
