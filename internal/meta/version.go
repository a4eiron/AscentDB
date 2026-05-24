package meta

import (
	"bytes"
	"encoding/binary"
)

type Version struct {
	Levels [][]*TableMeta
}

type VersionEdit struct {
	AddTables    []*TableMeta
	DeleteTables []*DeletedTableMeta

	LastSequence *uint64
	NextFileNum  *uint64
	LogNumber    *uint64
}

func encodeVersionEdit(edit *VersionEdit) []byte {

	buf := new(bytes.Buffer)
	tmp := make([]byte, binary.MaxVarintLen64)

	writeTag := func(tag uint64) {
		n := binary.PutUvarint(tmp, tag)
		buf.Write(tmp[:n])
	}

	for _, add := range edit.AddTables {
		writeTag(tagAddTable)
		b := encodeTableMeta(add)
		buf.Write(b)
	}

	for _, del := range edit.DeleteTables {
		writeTag(tagDeleteTable)
		b := encodeDeletedTable(del)
		buf.Write(b)
	}

	if edit.LogNumber != nil {
		writeTag(tagLogNum)
		n := binary.PutUvarint(tmp, *edit.LogNumber)
		buf.Write(tmp[:n])
	}

	if edit.LastSequence != nil {
		writeTag(tagLastSequence)
		n := binary.PutUvarint(tmp, *edit.LastSequence)
		buf.Write(tmp[:n])
	}

	if edit.NextFileNum != nil {
		writeTag(tagNextFileNum)
		n := binary.PutUvarint(tmp, *edit.NextFileNum)
		buf.Write(tmp[:n])
	}
	return buf.Bytes()
}
