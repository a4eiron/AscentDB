package meta

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"sync"
)

type VersionSet struct {
	Current *Version

	nextFileNum uint64
	lastSeqNum  uint64
	logNum      uint64

	manifest *os.File
	mu       sync.Mutex
}

func (vs *VersionSet) LogAndApply(edit *VersionEdit) error {

	vs.mu.Lock()
	defer vs.mu.Unlock()

	b := encodeVersionEdit(edit)

	_, err := vs.manifest.Write(b)
	if err != nil {
		return err
	}

	if err := vs.manifest.Sync(); err != nil {
		return err
	}

	newVersion := vs.apply(edit)
	vs.Current = newVersion

	return nil
}

func (vs *VersionSet) NextFileNum() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	n := vs.nextFileNum
	vs.nextFileNum++

	return n
}

func (vs *VersionSet) LastSequenceNum() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	return vs.lastSeqNum
}

func (vs *VersionSet) LogNumber() uint64 {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	return vs.logNum
}

func (vs *VersionSet) apply(edit *VersionEdit) *Version {
	newVersion := &Version{Levels: make([][]*TableMeta, len(vs.Current.Levels))}

	for i := range vs.Current.Levels {
		newVersion.Levels[i] = append(newVersion.Levels[i], vs.Current.Levels[i]...)
	}

	// remove deleted
	for _, del := range edit.DeleteTables {
		lvl := del.Level
		for i, t := range newVersion.Levels[lvl] {
			if t.FileNum == del.FileNum {
				newVersion.Levels[lvl] = append(
					newVersion.Levels[lvl][:i],
					newVersion.Levels[lvl][i+1:]...)
				break
			}
		}
	}
	// add new
	for _, add := range edit.AddTables {
		newVersion.Levels[add.Level] = append(newVersion.Levels[add.Level], add)

	}
	return newVersion
}

func (vs *VersionSet) replay(f *os.File) error {

	data, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	reader := bytes.NewReader(data)
	for reader.Len() > 0 {
		tag, err := binary.ReadUvarint(reader)
		if err != nil {
			return err
		}

		switch tag {
		case tagAddTable:
			var size uint32
			if err := binary.Read(reader, binary.LittleEndian, &size); err != nil {
				return err
			}
			reader.Seek(-4, io.SeekCurrent)

			buf := make([]byte, size)
			if _, err := reader.Read(buf); err != nil {
				return err
			}

			tableMeta, err := decodeTableMeta(buf)
			if err != nil {
				return err
			}

			vs.Current.Levels[tableMeta.Level] = append(vs.Current.Levels[tableMeta.Level], tableMeta)

		case tagDeleteTable:
			buf := make([]byte, 12)
			if _, err := reader.Read(buf); err != nil {
				return err
			}

			deletedTableMeta, err := decodeDeletedTable(buf)
			if err != nil {
				return err
			}
			vs.removeFromLevel(deletedTableMeta.Level, deletedTableMeta.FileNum)

		case tagLastSequence:
			val, _ := binary.ReadUvarint(reader)
			vs.lastSeqNum = val

		case tagNextFileNum:
			val, _ := binary.ReadUvarint(reader)
			vs.nextFileNum = val

		case tagLogNum:
			val, _ := binary.ReadUvarint(reader)
			vs.logNum = val
		}

	}

	return nil
}

func (vs *VersionSet) removeFromLevel(level uint32, fileNum uint64) {
	for i, f := range vs.Current.Levels[level] {
		if f.FileNum == fileNum {
			vs.Current.Levels[level] = append(
				vs.Current.Levels[level][:i],
				vs.Current.Levels[level][i+1:]...,
			)
		}
	}
}
