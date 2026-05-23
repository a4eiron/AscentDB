package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/a4eiron/ascentdb/internal/record"
)

type TableReader struct {
	file    *os.File
	index   *IndexBlock
	filter  *Filter
	footer  *Footer
	cache   *BlockCache
	fileNum uint64
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

	footerBytes, err := readAt(file, stat.Size()-int64(FooterSize), uint32(FooterSize))
	if err != nil {
		return nil, err
	}

	footer, err := decodeFooter(footerBytes)
	if err != nil {
		return nil, err
	}

	indexBytes, err := readAt(file, int64(footer.IndexOffset), footer.IndexSize)
	if err != nil {
		return nil, err
	}

	index, err := decodeIndexBlock(indexBytes)
	if err != nil {
		return nil, err
	}

	filterBytes, err := readAt(file, int64(footer.FilterOffset), footer.FilterSize)
	if err != nil {
		return nil, err
	}

	filter, err := DecodeFilter(filterBytes)
	if err != nil {
		return nil, err
	}

	var fileNum uint64
	base := filepath.Base(file.Name())
	fmt.Sscanf(base, "table-%06d.sst", &fileNum)

	reader := &TableReader{
		file:    file,
		index:   index,
		filter:  filter,
		footer:  footer,
		cache:   NewBlockCache(30),
		fileNum: fileNum,
	}

	return reader, nil
}

func (r *TableReader) Get(key record.InternalKey) (*record.Record, bool, error) {

	if !r.filter.Contains(key.UserKey) {
		return nil, false, nil
	}
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

func (r *TableReader) Close() error {
	return r.file.Close()
}

func (r *TableReader) readBlock(offset uint64, size uint32) (*Block, error) {
	if r.cache != nil {
		if data, ok := r.cache.Get(r.fileNum, offset); ok {
			return decodeBlock(data)
		}
	}

	blockBytes := make([]byte, size)
	_, err := r.file.ReadAt(blockBytes, int64(offset))
	if err != nil {
		return nil, err
	}

	if r.cache != nil {
		r.cache.Set(r.fileNum, offset, blockBytes)
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

func (r *TableReader) findBlockIndex(key record.InternalKey) int {
	return sort.Search(len(r.index.entries), func(i int) bool {
		return r.index.entries[i].SeparatorKey.Compare(key) >= 0
	})
}

func readAt(file *os.File, offset int64, size uint32) ([]byte, error) {
	b := make([]byte, size)
	_, err := file.ReadAt(b, offset)
	return b, err
}
