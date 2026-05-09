package meta

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/a4eiron/ascentdb/internal/codec"
	"github.com/a4eiron/ascentdb/internal/record"
)

const currentFile = "CURRENT"
const TableMetaSize = 4 + 8 + 8 + 4 + 4 + 4

type ManifestRecordType uint8

const (
	tagAddTable     = 1
	tagDeleteTable  = 2
	tagLastSequence = 3
	tagNextFileNum  = 4
	tagLogNum       = 5
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

func Open(dataDir string) (*VersionSet, error) {

	currentPath := filepath.Join(dataDir, currentFile)
	manifestName, err := os.ReadFile(currentPath)

	var manifestFile *os.File
	vs := &VersionSet{
		Current: &Version{
			Levels: make([][]*TableMeta, 7),
		},
	}

	if err != nil {
		if os.IsNotExist(err) {
			return newManifest(dataDir, vs)
		}
		return nil, err
	}

	manifestPath := filepath.Join(dataDir, string(manifestName))
	manifestFile, err = os.OpenFile(manifestPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	if err := vs.replay(manifestFile); err != nil {
		return nil, err
	}

	vs.manifest = manifestFile
	return vs, nil

}

func newManifest(dataDir string, vs *VersionSet) (*VersionSet, error) {
	name := fmt.Sprintf("MANIFEST-%06d", 1)
	f, err := os.Create(filepath.Join(dataDir, name))
	if err != nil {
		return nil, err
	}

	err = os.WriteFile(filepath.Join(dataDir, currentFile), []byte(name), 0644)
	if err != nil {
		return nil, err
	}

	vs.manifest = f
	vs.nextFileNum = 1

	return vs, nil
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
	buf.WriteUint32(uint32(len(m.MinKey.UserKey)))
	buf.WriteBytes([]byte(m.MinKey.UserKey))
	buf.WriteUint64(m.MinKey.SeqNum)
	buf.WriteUint8(uint8(m.MinKey.Type))

	buf.WriteUint32(maxKeySize)
	buf.WriteUint32(uint32(len(m.MaxKey.UserKey)))
	buf.WriteBytes([]byte(m.MaxKey.UserKey))
	buf.WriteUint64(m.MaxKey.SeqNum)
	buf.WriteUint8(uint8(m.MaxKey.Type))

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
	minKeySize, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	minUserKeySize, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	minUserKeyBytes, err := buf.ReadBytes(int(minUserKeySize))
	if err != nil {
		return nil, err
	}

	minSeqNum, err := buf.ReadUint64()
	if err != nil {
		return nil, err
	}

	minKeyType, err := buf.ReadUint8()
	if err != nil {
		return nil, err
	}

	// --------
	maxKeySize, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	maxUserKeySize, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	maxUserKeyBytes, err := buf.ReadBytes(int(maxUserKeySize))
	if err != nil {
		return nil, err
	}

	maxSeqNum, err := buf.ReadUint64()
	if err != nil {
		return nil, err
	}

	maxKeyType, err := buf.ReadUint8()
	if err != nil {
		return nil, err
	}

	t := &TableMeta{
		FileNum:  fileNum,
		FileSize: fileSize,
		Level:    level,

		MinKey: record.InternalKey{
			UserKey: string(minUserKeyBytes),
			SeqNum:  minSeqNum,
			Type:    record.IKType(minKeyType),
		},

		MaxKey: record.InternalKey{
			UserKey: string(maxUserKeyBytes),
			SeqNum:  maxSeqNum,
			Type:    record.IKType(maxKeyType),
		},
	}

	log.Println("minkeysize:", minKeySize, t.MinKey.KeySize())
	log.Println("maxkeysize:", maxKeySize, t.MaxKey.KeySize())

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
