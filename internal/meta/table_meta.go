package meta

import (
	"encoding/binary"

	"github.com/a4eiron/ascentdb/internal/record"
)

type TableMeta struct {
	FileNum  uint64
	FileSize uint64
	Level    uint32
	MinKey   record.InternalKey
	MaxKey   record.InternalKey
}

type DeletedTableMeta struct {
	Level   uint32
	FileNum uint64
}

// [total_size(4)][file_num(8)][file_size(8)][level(4)]
// [min_key_size(4)][min_key...][max_key_size(4)][max_key...]
func encodeTableMeta(m *TableMeta) []byte {
	minKeySize := int(m.MinKey.KeySize())
	maxKeySize := int(m.MaxKey.KeySize())
	totalSize := TableMetaSize + minKeySize + maxKeySize

	buf := make([]byte, totalSize)
	off := 0

	binary.LittleEndian.PutUint32(buf[off:], uint32(totalSize))
	off += 4
	binary.LittleEndian.PutUint64(buf[off:], m.FileNum)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], m.FileSize)
	off += 8
	binary.LittleEndian.PutUint32(buf[off:], m.Level)
	off += 4

	binary.LittleEndian.PutUint32(buf[off:], uint32(minKeySize))
	off += 4
	off = record.EncodeInternalKey(buf, off, m.MinKey)

	binary.LittleEndian.PutUint32(buf[off:], uint32(maxKeySize))
	off += 4
	off = record.EncodeInternalKey(buf, off, m.MaxKey)

	_ = off
	return buf
}

func decodeTableMeta(b []byte) (*TableMeta, error) {
	if len(b) < TableMetaSize {
		return nil, record.ErrCorruptRecord
	}
	off := 0
	off += 4

	fileNum := binary.LittleEndian.Uint64(b[off:])
	off += 8

	fileSize := binary.LittleEndian.Uint64(b[off:])
	off += 8

	level := binary.LittleEndian.Uint32(b[off:])
	off += 4

	off += 4
	var err error
	var minKey, maxKey record.InternalKey

	minKey, off, err = record.DecodeInternalKey(b, off)
	if err != nil {
		return nil, err
	}

	off += 4
	maxKey, _, err = record.DecodeInternalKey(b, off)
	if err != nil {
		return nil, err
	}

	return &TableMeta{
		FileNum:  fileNum,
		FileSize: fileSize,
		Level:    level,
		MinKey:   minKey,
		MaxKey:   maxKey,
	}, nil
}

// [level(4)][file_num(8)]
func encodeDeletedTable(m *DeletedTableMeta) []byte {
	buf := make([]byte, 4+8)
	binary.LittleEndian.PutUint32(buf[0:], m.Level)
	binary.LittleEndian.PutUint64(buf[4:], m.FileNum)
	return buf
}

func decodeDeletedTable(b []byte) (*DeletedTableMeta, error) {
	if len(b) < 12 {
		return nil, record.ErrCorruptRecord
	}
	return &DeletedTableMeta{
		Level:   binary.LittleEndian.Uint32(b[0:]),
		FileNum: binary.LittleEndian.Uint64(b[4:]),
	}, nil
}
