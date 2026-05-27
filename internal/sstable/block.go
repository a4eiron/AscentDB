package sstable

import (
	"encoding/binary"
	"hash/crc32"

	"github.com/a4eiron/ascentdb/internal/record"
)

type Block struct {
	entries []record.Record
}

// [block_size(4)][num_entries(4)][[entry_size(4)][entry]...][checksum(4)]
func encodeBlock(b *Block, size int) []byte {
	size += 4
	buf := make([]byte, size)
	off := 0

	binary.LittleEndian.PutUint32(buf[off:], uint32(size))
	off += 4

	binary.LittleEndian.PutUint32(buf[off:], uint32(len(b.entries)))
	off += 4

	for _, entry := range b.entries {
		entryBytes := record.EncodeRecord(entry)
		binary.LittleEndian.PutUint32(buf[off:], uint32(len(entryBytes)))
		off += 4
		copy(buf[off:], entryBytes)
		off += len(entryBytes)
	}

	crc := crc32.ChecksumIEEE(buf[:size-4])
	binary.LittleEndian.PutUint32(buf[size-4:], crc)

	return buf
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

// func blockSize(b Block) int {
// 	size := 4 + 4 + len(b.entries)*4
// 	for _, entry := range b.entries {
// 		size += int(entry.Size())
// 	}
// 	return size
// }
