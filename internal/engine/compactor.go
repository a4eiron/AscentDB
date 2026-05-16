package engine

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/a4eiron/ascentdb/internal/compaction"
	"github.com/a4eiron/ascentdb/internal/meta"
	"github.com/a4eiron/ascentdb/internal/record"
	"github.com/a4eiron/ascentdb/internal/sstable"
)

func (e *Engine) scheduleCompaction() {
	fileNum := e.vs.NextFileNum()

	l0 := append([]*meta.TableMeta(nil), e.vs.Current.Levels[0]...)

	if len(l0) > 2 {
		log.Println("Compating L0)")
		time.Sleep(4 * time.Second)
		go e.CompactL0(e.opts.DataDir, l0, fileNum)
	}
}

func (e *Engine) CompactL0(dataDir string, l0 []*meta.TableMeta, fileNum uint64) {

	var iters []*sstable.Iterator

	var deletedTablesMeta []*meta.DeletedTableMeta

	// open iterators on Level - 0
	for i := len(l0) - 1; i >= 0; i-- {
		t := l0[i]
		path := filepath.Join(dataDir, "tables", fmt.Sprintf("table-%06d", t.FileNum))
		reader, err := sstable.Open(path)
		if err != nil {
			log.Println(err)
		}

		iters = append(iters, reader.Iterator())
		deletedTablesMeta = append(deletedTablesMeta, &meta.DeletedTableMeta{
			Level:   t.Level,
			FileNum: t.FileNum,
		})
	}

	outPath := filepath.Join(dataDir, "tables", fmt.Sprintf("table-%06d", fileNum))
	writer, err := sstable.Create(outPath, 64*1024)
	if err != nil {
		log.Println(err)
	}

	merger := compaction.NewMergeIterator(iters)

	var lastUserKey string
	first := true
	var firstKey, lastKey record.InternalKey

	for merger.Valid() {
		rec := merger.Record()

		if first {
			firstKey = rec.InternalKey
			first = false
		}
		lastKey = rec.InternalKey

		// de-dup
		if rec.UserKey == lastUserKey {
			merger.Next()
			continue
		}

		lastUserKey = rec.UserKey

		// drop tombstones
		// if rec.Type == record.TypeDel {
		// 	merger.Next()
		// 	continue
		// }

		if err := writer.Add(*rec); err != nil {
			log.Println(err)
		}

		merger.Next()
	}

	fileSize, err := writer.Size()
	if err != nil {
		log.Println(err)
	}

	if err := writer.Close(); err != nil {
		log.Println(err)
	}

	e.mu.Lock()
	nextFileNum := e.vs.NextFileNum()
	e.mu.Unlock()

	edit := &meta.VersionEdit{
		NextFileNum:  &nextFileNum,
		LastSequence: &lastKey.SeqNum,
		DeleteTables: deletedTablesMeta,
		AddTables: []*meta.TableMeta{
			{
				FileNum:  fileNum,
				FileSize: uint64(fileSize),
				Level:    1,
				MinKey:   firstKey,
				MaxKey:   lastKey,
			},
		},
	}

	e.mu.Lock()
	if err := e.vs.LogAndApply(edit); err != nil {
		log.Println(err)
	}

	for _, t := range l0 {
		tNum := t.FileNum
		err := os.Remove(filepath.Join(dataDir, "tables", fmt.Sprintf("table-%06d", tNum)))
		if err != nil {
			log.Println(err)
		}
	}

	e.mu.Unlock()

}
