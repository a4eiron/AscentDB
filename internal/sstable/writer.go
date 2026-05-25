package sstable

import (
	"os"

	"github.com/a4eiron/ascentdb/internal/record"
)

type TableWriter struct {
	file             *os.File
	blockSize        int
	block            *Block
	currentBlockSize int
	index            *IndexBlock
	filter           *Filter
	offset           uint64
	estimatedSize    uint64
}

func Create(path string, blockSize int) (*TableWriter, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	return &TableWriter{
		file:             file,
		blockSize:        blockSize,
		currentBlockSize: 8,
		block:            &Block{entries: make([]record.Record, 0)},
		index:            &IndexBlock{entries: make([]IndexEntry, 0)},
		filter:           NewFilter(1000, 0.01),
	}, nil
}

func (w *TableWriter) Add(r record.Record) error {
	w.block.entries = append(w.block.entries, r)

	w.currentBlockSize += 4             // entry size
	w.currentBlockSize += int(r.Size()) // encoded record

	w.estimatedSize += uint64(r.Size())
	w.filter.Add(r.UserKey)

	if int(w.currentBlockSize) >= w.blockSize {
		return w.flushBlock()
	}

	return nil
}

func (w *TableWriter) Path() string {
	return w.file.Name()
}

func (w *TableWriter) EstimatedSize() (int64, error) {
	return int64(w.estimatedSize), nil
}

func (w *TableWriter) flushBlock() error {
	if len(w.block.entries) < 1 {
		return nil
	}

	encodedBlock := encodeBlock(w.block, w.currentBlockSize)
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
	w.currentBlockSize = 8

	return nil
}

func (w *TableWriter) Close() (int64, error) {

	if err := w.flushBlock(); err != nil {
		return 0, err
	}

	indexOffset := w.offset
	indexBytes := encodeIndexBlock(w.index)

	indexSize, err := w.file.Write(indexBytes)
	if err != nil {
		return 0, err
	}

	w.offset += uint64(indexSize)

	filterOffset := w.offset
	filterBytes := EncodeFilter(w.filter)
	filterSize, err := w.file.Write(filterBytes)
	if err != nil {
		return 0, err
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
		return 0, err
	}

	if err := w.file.Sync(); err != nil {
		return 0, err
	}

	stat, err := w.file.Stat()
	if err != nil {
		return 0, err
	}
	size := stat.Size()

	return size, w.file.Close()
}
