package sstable

import (
	"os"
	"sort"

	"github.com/a4eiron/ascentdb/internal/record"
)

type TableReader struct {
	file   *os.File
	index  *IndexBlock
	footer *Footer
}

func Open(path string) (*TableReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}

	footerBytes := make([]byte, FooterSize)

	_, err = file.ReadAt(footerBytes, stat.Size()-int64(FooterSize))
	if err != nil {
		return nil, err
	}

	footer, err := decodeFooter(footerBytes)
	if err != nil {
		return nil, err
	}

	indexBytes := make([]byte, footer.IndexSize)
	_, err = file.ReadAt(indexBytes, int64(footer.IndexOffset))
	if err != nil {
		return nil, err
	}

	index, err := decodeIndexBlock(indexBytes)
	if err != nil {
		return nil, err
	}

	reader := &TableReader{
		file:   file,
		index:  index,
		footer: footer,
	}

	return reader, nil
}

func (r *TableReader) Get(key record.InternalKey) (*record.Record, bool, error) {
	entry := r.findBlock(key)
	if entry == nil {
		return nil, false, nil
	}

	block, err := r.readBlock(entry.BlockOffset, entry.BlockSize)
	if err != nil {
		return nil, false, err
	}

	idx := sort.Search(len(block.entries), func(i int) bool {
		return block.entries[i].Compare(key) >= 0
	})

	if idx >= len(block.entries) ||
		block.entries[idx].UserKey != key.UserKey {
		return nil, false, nil
	}

	rec := block.entries[idx]
	return &rec, true, nil
}

func (r *TableReader) readBlock(offset uint64, size uint32) (*Block, error) {
	blockBytes := make([]byte, size)
	_, err := r.file.ReadAt(blockBytes, int64(offset))
	if err != nil {
		return nil, err
	}
	return decodeBlock(blockBytes)
}

func (r *TableReader) findBlock(key record.InternalKey) *IndexEntry {

	idx := sort.Search(len(r.index.entries), func(i int) bool {
		return r.index.entries[i].SeparatorKey.Compare(key) >= 0
	})

	if idx >= len(r.index.entries) {
		return nil
	}
	return &r.index.entries[idx]
}
