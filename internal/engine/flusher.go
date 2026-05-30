package engine

import (
	"fmt"
	"log"
	"os"

	"github.com/a4eiron/ascentdb/internal/memtable"
	"github.com/a4eiron/ascentdb/internal/meta"
	"github.com/a4eiron/ascentdb/internal/record"
	"github.com/a4eiron/ascentdb/internal/sstable"
	"github.com/a4eiron/ascentdb/internal/wal"
)

type flushTask struct {
	oldWalPath string
	oldWal     *wal.WAL

	mt *memtable.Memtable

	writer  *sstable.TableWriter
	fileNum uint64
}

func (e *Engine) rotate() (*flushTask, error) {
	mt := e.mt
	e.immt = mt

	var (
		oldWal     *wal.WAL
		oldWalPath string
	)

	if e.opts.CrashRecovery {
		oldWal = e.wal
		e.imwal = oldWal
		oldWalPath = oldWal.Path()

		logNum := e.vs.NextFileNum()

		newWal, err := wal.Open(
			e.walPath(logNum),
			e.opts.SyncOptions,
		)
		if err != nil {
			log.Println("failed to create wal:", err)
			return nil, err
		}

		e.wal = newWal

		edit := &meta.VersionEdit{
			LogNumber: &logNum,
		}

		if err := e.vs.LogAndApply(edit); err != nil {
			log.Println(err)
		}
		// log.Println("rotate logNumber:", logNum)
	}

	e.mt = memtable.New(uint64(e.opts.MemtableSize))

	fileNum := e.vs.NextFileNum()

	e.ensureLevelDir(0)

	writer, err := sstable.Create(
		e.tablePath(0, fileNum),
		e.opts.BlockSize,
	)
	if err != nil {
		return nil, err
	}

	return &flushTask{
		oldWal:     oldWal,
		oldWalPath: oldWalPath,
		mt:         mt,
		writer:     writer,
		fileNum:    fileNum,
	}, nil
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
	var (
		firstKey record.InternalKey
		lastKey  record.InternalKey
		hasData  bool
	)

	iter := task.mt.Iterator()

	for iter.Valid() {
		key := iter.Key()
		value := iter.Value()

		if !hasData {
			firstKey = key
			hasData = true
		}

		lastKey = key

		if err := task.writer.Add(record.Record{
			InternalKey: key,
			Value:       value,
		}); err != nil {
			return fmt.Errorf("failed to add record to sstable: %w", err)
		}

		iter.Next()
	}

	if !hasData {
		_, _ = task.writer.Close()
		return os.Remove(task.writer.Path())
	}

	fileSize, err := task.writer.Close()
	if err != nil {
		return fmt.Errorf("failed to close sstable: %w", err)
	}

	nextFileNum := e.vs.NextFileNum()

	edit := &meta.VersionEdit{
		NextFileNum:  &nextFileNum,
		LastSequence: &lastKey.SeqNum,
		AddTables: []*meta.TableMeta{
			{
				FileNum:  task.fileNum,
				FileSize: uint64(fileSize),
				Level:    0,
				MinKey:   firstKey,
				MaxKey:   lastKey,
			},
		},
	}

	e.mu.Lock()
	if err := e.vs.LogAndApply(edit); err != nil {
		log.Println(err)
	}

	if task.oldWal != nil {
		if err := task.oldWal.Close(); err != nil {
			log.Println("failed to close wal:", task.oldWalPath, err)
		}

		if err := os.Remove(task.oldWalPath); err != nil {
			log.Println("failed to delete wal:", task.oldWalPath, err)
		}
	}

	e.immt = nil
	e.imwal = nil
	if len(e.vs.Current.Levels[0]) >= maxL0Files {
		e.scheduleCompaction()
	}

	e.mu.Unlock()
	return nil
}
