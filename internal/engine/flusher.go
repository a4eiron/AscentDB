package engine

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/a4eiron/ascentdb/internal/memtable"
	"github.com/a4eiron/ascentdb/internal/meta"
	"github.com/a4eiron/ascentdb/internal/record"
	"github.com/a4eiron/ascentdb/internal/sstable"
	"github.com/a4eiron/ascentdb/internal/wal"
)

type flushTask struct {
	oldWalPath string
	oldWal     *wal.WAL
	mt         *memtable.Memtable
	writer     *sstable.TableWriter
}

func (e *Engine) rotate() (*flushTask, error) {

	blockSize := e.opts.BlockSize

	// capture the active memtable and wal
	mt := e.mt
	oldWal := e.wal

	// make them immutable
	e.immt = mt
	e.imwal = oldWal

	// crate  new memtable and wal
	fileNum := e.vs.NextFileNum()

	newWal, err := wal.Open(e.walPath(fileNum), e.opts.WALSyncInterval)
	if err != nil {
		log.Println("failed to create new WAL", err)
		return nil, err
	}

	edit := &meta.VersionEdit{LogNumber: &fileNum}
	if err := e.vs.LogAndApply(edit); err != nil {
		log.Println(err)
	}

	e.mt = memtable.New(uint64(e.opts.MemtableSize))
	e.wal = newWal

	// create an sstable writer
	fileNum = e.vs.NextFileNum()
	e.ensureLevelDir(0)
	path := e.tablePath(0, fileNum)

	writer, err := sstable.Create(path, blockSize)
	if err != nil {
		return nil, err
	}

	task := &flushTask{
		oldWal:     oldWal,
		oldWalPath: oldWal.Path(),
		mt:         mt,
		writer:     writer,
	}

	return task, nil
}

func (e *Engine) runFlusher() {
	for task := range e.flushChan {
		if err := e.flush(task); err != nil {
			log.Println(err)
		}
		e.flushWg.Done()
	}
}

func (e *Engine) flush(task *flushTask) error {

	first := true
	var firstKey, lastKey *record.InternalKey

	for iter := task.mt.Iterator(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()

		if first {
			firstKey = key
			first = false
		}
		lastKey = key

		if err := task.writer.Add(record.Record{
			InternalKey: key,
			Value:       value,
		}); err != nil {
			return fmt.Errorf("failed to write to add to sstable: %w", err)
		}
	}

	var fileNum uint64
	fmt.Sscanf(filepath.Base(task.writer.Path()), "table-%06d.sst", &fileNum)

	// fileSize, err := task.writer.Size()
	// if err != nil {
	// 	return fmt.Errorf("failed to get writer size: %w", err)
	// }

	fileSize, err := task.writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close sstable: %w", err)
	}

	nextFileNum := e.vs.NextFileNum()

	if first {
		task.writer.Close()
		return nil
	}

	edit := &meta.VersionEdit{
		NextFileNum:  &nextFileNum,
		LastSequence: &lastKey.SeqNum,
		AddTables: []*meta.TableMeta{
			{
				FileNum:  fileNum,
				FileSize: uint64(fileSize),
				Level:    0,
				MinKey:   *firstKey,
				MaxKey:   *lastKey,
			},
		},
	}

	e.mu.Lock()
	if err := e.vs.LogAndApply(edit); err != nil {
		log.Println(err)
	}

	if err := task.oldWal.Close(); err != nil {
		log.Println("failed to close wal:", task.oldWalPath, err)
	}
	if err := os.Remove(task.oldWalPath); err != nil {
		log.Println("failed to delete wal:", task.oldWalPath, err)
	}

	e.immt = nil
	e.imwal = nil
	if len(e.vs.Current.Levels[0]) >= maxL0Files {
		e.scheduleCompaction()
	}

	e.mu.Unlock()
	return nil
}
