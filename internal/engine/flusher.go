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
	oldWalPath   string
	oldWal       *wal.WAL
	mt           *memtable.Memtable
	writer       *sstable.TableWriter
	manifestPath string
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

	// log.Println("wal ", fileNum)
	// time.Sleep(4 * time.Second)

	newWal, err := wal.Open(filepath.Join(e.opts.DataDir, "wal", fmt.Sprintf("wal-%06d", fileNum)))
	if err != nil {
		log.Println("failed to create new WAL", err)
		return nil, err
	}

	edit := &meta.VersionEdit{LogNumber: &fileNum}
	if err := e.vs.LogAndApply(edit); err != nil {
		log.Println(err)
	}

	e.mt = memtable.New(64 * 1024)
	e.wal = newWal

	// create an sstable writer
	fileNum = e.vs.NextFileNum()
	path := filepath.Join(e.opts.DataDir, "tables", fmt.Sprintf("table-%06d", fileNum))

	writer, err := sstable.Create(path, blockSize)
	if err != nil {
		return nil, err
	}

	fileNum = e.vs.NextFileNum()
	// log.Println("sstable ", fileNum)
	// time.Sleep(4 * time.Second)

	task := &flushTask{
		oldWal:       oldWal,
		oldWalPath:   oldWal.Path(),
		mt:           mt,
		writer:       writer,
		manifestPath: filepath.Join(e.opts.DataDir, fmt.Sprintf("MANIFEST-%06d", fileNum)),
	}

	return task, nil
}

func (e *Engine) runFlusher() {

	for task := range e.flushChan {

		first := true
		var firstKey, lastKey record.InternalKey

		for iter := task.mt.Iterator(); iter.Valid(); iter.Next() {
			key := iter.Key()
			value := iter.Value()

			if first {
				firstKey = key
				first = false
			}

			log.Println("FLUSH:", key)
			err := task.writer.Add(record.Record{
				InternalKey: key,
				Value:       value,
			})
			lastKey = key

			if err != nil {
				log.Println("failed to write to add to sstable:", err)
			}
		}

		nextFileNum := e.vs.NextFileNum()
		var fileNum uint64
		base := filepath.Base(task.writer.Path())
		fmt.Sscanf(base, "table-%06d", &fileNum)
		fileSize, err := task.writer.Size()
		if err != nil {
			log.Println(err)
		}

		if err := task.writer.Close(); err != nil {
			log.Println("failed to close sstable:", err)
		}

		edit := &meta.VersionEdit{
			NextFileNum:  &nextFileNum,
			LastSequence: &lastKey.SeqNum,
			AddTables: []*meta.TableMeta{
				{
					FileNum:  fileNum,
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

		if err := task.oldWal.Close(); err != nil {
			log.Println("failed to close wal:", task.oldWalPath, err)
		}
		if err := os.Remove(task.oldWalPath); err != nil {
			log.Println("failed to delete wal:", task.oldWalPath, err)
		}

		e.immt = nil
		e.imwal = nil

		fileNum = e.vs.NextFileNum()
		if len(e.vs.Current.Levels[0]) > 4 {
			e.scheduleCompaction()
		}
		e.mu.Unlock()
	}

}
