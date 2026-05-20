package sstable

import (
	"os"

	"github.com/a4eiron/ascentdb/internal/record"
)

type TableWriter struct {
	file      *os.File
	blockSize int
	block     *Block
	index     *IndexBlock
	filter    *Filter
	offset    uint64
}

func Create(path string, blockSize int) (*TableWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &TableWriter{
		file:      file,
		blockSize: blockSize,
		block:     &Block{entries: make([]record.Record, 0)},
		index:     &IndexBlock{entries: make([]IndexEntry, 0)},
		filter:    NewFilter(1000, 0.01),
	}, nil
}

func (w *TableWriter) Add(r record.Record) error {
	w.block.entries = append(w.block.entries, r)

	w.filter.Add(r.UserKey)

	if blockSize(*w.block) >= w.blockSize {
		return w.flushBlock()
	}

	return nil
}

func (w *TableWriter) Path() string {
	return w.file.Name()
}

func (w *TableWriter) Size() (int64, error) {
	stat, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	size := stat.Size()
	return size, nil
}

func (w *TableWriter) flushBlock() error {
	if len(w.block.entries) < 1 {
		return nil
	}

	encodedBlock := encodeBlock(w.block)
	offset := w.offset

	n, err := w.file.Write(encodedBlock)
	if err != nil {
		return err
	}

	last := w.block.entries[len(w.block.entries)-1]

	w.index.entries = append(w.index.entries, IndexEntry{
		SeparatorKey: last.InternalKey,
		BlockOffset:  offset,
		BlockSize:    uint32(n),
	})

	w.offset += uint64(n)

	w.block.entries = w.block.entries[:0]

	return nil
}

func (w *TableWriter) Close() error {

	if err := w.flushBlock(); err != nil {
		return err
	}

	indexOffset := w.offset
	indexBytes := encodeIndexBlock(w.index)

	indexSize, err := w.file.Write(indexBytes)
	if err != nil {
		return err
	}

	w.offset += uint64(indexSize)

	filterOffset := w.offset
	filterBytes := EncodeFilter(w.filter)
	filterSize, err := w.file.Write(filterBytes)
	if err != nil {
		return err
	}

	w.offset += uint64(filterSize)

	footer := Footer{
		IndexOffset:  indexOffset,
		IndexSize:    uint32(indexSize),
		FilterOffset: filterOffset,
		FilterSize:   uint32(filterSize),
		Magic:        Magic,
	}

	footerBytes := encodeFooter(footer)

	_, err = w.file.Write(footerBytes)
	if err != nil {
		return err
	}

	if err := w.file.Sync(); err != nil {
		return err
	}

	return w.file.Close()
}
