package sstable

import (
	"encoding/binary"
	"errors"
)

const Magic uint64 = 0xdbb5a3c4f1e2d678
const FooterSize int = 8 + 4 + 8 + 4 + 8

type Footer struct {
	IndexOffset  uint64
	IndexSize    uint32
	FilterOffset uint64
	FilterSize   uint32
	Magic        uint64
}

func encodeFooter(f Footer) []byte {
	buf := make([]byte, FooterSize)

	off := 0

	binary.LittleEndian.PutUint64(buf[off:], f.IndexOffset)
	off += 8

	binary.LittleEndian.PutUint32(buf[off:], f.IndexSize)
	off += 4

	binary.LittleEndian.PutUint64(buf[off:], f.FilterOffset)
	off += 8

	binary.LittleEndian.PutUint32(buf[off:], f.FilterSize)
	off += 4

	binary.LittleEndian.PutUint64(buf[off:], f.Magic)
	return buf
}

func decodeFooter(b []byte) (*Footer, error) {
	if len(b) < FooterSize {
		return nil, errors.New("footer: buffer too small")
	}

	off := 0

	indexOffset := binary.LittleEndian.Uint64(b[off:])
	off += 8

	indexSize := binary.LittleEndian.Uint32(b[off:])
	off += 4

	filterOffset := binary.LittleEndian.Uint64(b[off:])
	off += 8

	filterSize := binary.LittleEndian.Uint32(b[off:])
	off += 4

	magic := binary.LittleEndian.Uint64(b[off:])
	if magic != Magic {
		return nil, errors.New("footer: corrupt sstable")
	}

	return &Footer{
		IndexOffset:  indexOffset,
		IndexSize:    indexSize,
		FilterOffset: filterOffset,
		FilterSize:   filterSize,
		Magic:        magic,
	}, nil
}
