package engine

import (
	"fmt"
	"log"
	"path/filepath"

	"github.com/a4eiron/ascentdb/internal/memtable"
	"github.com/a4eiron/ascentdb/internal/record"
	"github.com/a4eiron/ascentdb/internal/sstable"
)

const BLOCK_SIZE = 32 * 1024

type flushTask struct {
	path   string
	mt     *memtable.Memtable
	writer *sstable.TableWriter
}

func (e *Engine) rotate() error {

	blockSize := e.opts.BlockSize
	e.fileNum++
	fileNum := e.fileNum

	// capture the active memtable
	mt := e.mt

	// make it immutable
	e.immt = mt

	// crate a new memtable
	e.mt = memtable.New(64 * 1024)

	// create an sstable writer
	path := filepath.Join(e.opts.DataDir, "tables", fmt.Sprintf("table-%06d", fileNum))

	writer, err := sstable.Create(path, blockSize)
	if err != nil {
		return err
	}

	e.flushChan <- &flushTask{
		path:   path,
		mt:     mt,
		writer: writer,
	}

	return nil
}

func (e *Engine) runFlusher() {

	for task := range e.flushChan {

		for iter := task.mt.Iterator(); iter.Valid(); iter.Next() {
			err := task.writer.Add(record.Record{
				InternalKey: iter.Key(),
				Value:       iter.Value(),
			})
			if err != nil {
				log.Println("failed to write to add to sstable:", err)
			}
		}

		if err := task.writer.Close(); err != nil {
			log.Println("failed to close sstable:", err)
		}

		task.mt = nil
	}

}
