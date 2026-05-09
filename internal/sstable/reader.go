package sstable

import (
	"log"
	"os"

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

	defer func() {
		log.Println("inside sstable reader Get", key.UserKey)
	}()
	entry := r.findBlock(key)
	if entry == nil {

		return nil, false, nil
	}

	block, err := r.readBlock(entry.BlockOffset, entry.BlockSize)
	if err != nil {
		return nil, false, err
	}

	for _, rec := range block.entries {
		if rec.InternalKey.UserKey == key.UserKey {
			if rec.Type == record.TypeDel {
				return nil, false, nil
			}

			return &rec, true, nil
		}

	}
	return nil, false, nil
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
	for i, entry := range r.index.entries {
		if entry.SeparatorKey.Compare(key) >= 0 {
			return &r.index.entries[i]
		}
	}
	return nil
}
