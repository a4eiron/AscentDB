package meta

import (
	"github.com/a4eiron/ascentdb/internal/codec"
	"github.com/a4eiron/ascentdb/internal/record"
)

type TableMeta struct {
	FileNum  uint64
	FileSize uint64
	Level    uint32

	MinKey record.InternalKey
	MaxKey record.InternalKey
}

type DeletedTableMeta struct {
	Level   uint32
	FileNum uint64
}

func encodeTableMeta(m *TableMeta) []byte {

	minKeySize := m.MinKey.KeySize()
	maxKeySize := m.MaxKey.KeySize()
	totalSize := TableMetaSize + int(minKeySize) + int(maxKeySize)

	buf := codec.NewBuffer(totalSize)

	buf.WriteUint32(uint32(totalSize))
	buf.WriteUint64(m.FileNum)
	buf.WriteUint64(m.FileSize)
	buf.WriteUint32(m.Level)

	buf.WriteUint32(minKeySize)
	record.EncodeInternalKey(buf, m.MinKey)

	buf.WriteUint32(maxKeySize)
	record.EncodeInternalKey(buf, m.MaxKey)

	return buf.Bytes()
}

func decodeTableMeta(b []byte) (*TableMeta, error) {

	buf := codec.NewBufferFromBytes(b)
	_, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	fileNum, err := buf.ReadUint64()
	if err != nil {
		return nil, err
	}

	fileSize, err := buf.ReadUint64()
	if err != nil {
		return nil, err
	}
	level, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	// --------
	_, err = buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	minKey, err := record.DecodeInternalKey(buf)
	if err != nil {
		return nil, err
	}

	// --------
	_, err = buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	maxKey, err := record.DecodeInternalKey(buf)
	if err != nil {
		return nil, err
	}

	t := &TableMeta{
		FileNum:  fileNum,
		FileSize: fileSize,
		Level:    level,
		MinKey:   minKey,
		MaxKey:   maxKey,
	}

	return t, nil
}

func encodeDeletedTable(m *DeletedTableMeta) []byte {
	buf := codec.NewBuffer(4 + 8)
	buf.WriteUint32(m.Level)
	buf.WriteUint64(m.FileNum)

	return buf.Bytes()
}

func decodeDeletedTable(b []byte) (*DeletedTableMeta, error) {
	buf := codec.NewBufferFromBytes(b)
	level, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	fileNum, err := buf.ReadUint64()
	if err != nil {
		return nil, err
	}

	t := &DeletedTableMeta{
		Level:   level,
		FileNum: fileNum,
	}

	return t, nil
}
