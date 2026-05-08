package sstable

import (
	"os"

	"github.com/a4eiron/ascentdb/internal/record"
)

type TableWriter struct {
	file      *os.File
	blockSize int
	block     Block
	index     IndexBlock
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
		block:     Block{entries: make([]record.Record, 0)},
		index:     IndexBlock{entries: make([]IndexEntry, 0)},
	}, nil
}

func (w *TableWriter) Add(r record.Record) error {
	w.block.entries = append(w.block.entries, r)

	if blockSize(w.block) >= w.blockSize {
		return w.flushBlock()
	}

	return nil
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

	n, err := w.file.Write(indexBytes)
	if err != nil {
		return err
	}

	w.offset += uint64(n)

	footer := Footer{
		IndexOffset: indexOffset,
		IndexSize:   uint32(n),
		Magic:       Magic,
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
