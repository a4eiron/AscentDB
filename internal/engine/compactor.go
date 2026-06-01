package engine

import (
	"bytes"
	"log"
	"os"
	"sort"

	"github.com/a4eiron/ascentdb/internal/compaction"
	"github.com/a4eiron/ascentdb/internal/meta"
	"github.com/a4eiron/ascentdb/internal/record"
	"github.com/a4eiron/ascentdb/internal/sstable"
)

func (e *Engine) scheduleCompaction() {
	if !e.isCompacting.CompareAndSwap(false, true) {
		return
	}

	if len(e.vs.Current.Levels[0]) >= int(e.opts.MaxL0Files) {
		inputs := e.pickCompactionInputs(0)
		outFileNum := e.vs.NextFileNum()

		e.compactWg.Go(func() {
			e.compactLevel(0, inputs, outFileNum)
		})

		return
	}

	for level := 1; level < len(e.vs.Current.Levels)-1; level++ {
		if e.levelSize(level) <= e.levelCapacity(level) {
			continue
		}

		inputs := e.pickCompactionInputs(level)
		outFileNum := e.vs.NextFileNum()

		e.compactWg.Go(func() {
			e.compactLevel(level, inputs, outFileNum)
		})
		return
	}

	e.isCompacting.Store(false)
}

func (e *Engine) compactLevel(
	level int,
	inputs []*meta.TableMeta,
	outFileNum uint64,
) {
	nextLevel := level + 1

	minKey, maxKey := meta.KeyRangeOf(inputs)

	e.mu.Lock()
	nextLevelFiles := append([]*meta.TableMeta(nil), e.vs.Current.Levels[nextLevel]...)
	if level > 0 && len(inputs) > 0 {
		e.compactPointer[level] = inputs[len(inputs)-1].MaxKey.UserKey
	}
	e.mu.Unlock()

	overlapping := meta.FindOverlapping(nextLevelFiles, minKey, maxKey)

	all := append(inputs, overlapping...)
	iters := make([]*sstable.SSTableIterator, 0, len(all))
	readers := make([]*sstable.TableReader, 0, len(all))

	for _, t := range all {
		path := e.tablePath(int(t.Level), t.FileNum)
		reader, err := sstable.Open(path, e.blockCache)
		if err != nil {
			for _, r := range readers {
				r.Close()
			}
			return
		}
		readers = append(readers, reader)
		iters = append(iters, reader.Iterator())
	}

	isBottomLevel := nextLevel == len(e.vs.Current.Levels)-1

	outTables, err := e.writeCompactionOutput(outFileNum, nextLevel, iters, isBottomLevel)
	if err != nil {
		log.Println(err)
		for _, r := range readers {
			r.Close()
		}
		e.isCompacting.Store(false)
		return
	}

	for _, r := range readers {
		r.Close()
	}

	var deleted []*meta.DeletedTableMeta
	for _, t := range all {
		deleted = append(deleted, &meta.DeletedTableMeta{
			Level:   t.Level,
			FileNum: t.FileNum,
		})
	}

	nextFileNum := e.vs.NextFileNum()
	edit := &meta.VersionEdit{
		NextFileNum:  &nextFileNum,
		DeleteTables: deleted,
		AddTables:    outTables,
	}

	e.mu.Lock()
	if err := e.vs.LogAndApply(edit); err != nil {
		log.Println(err)
		e.isCompacting.Store(false)
		e.mu.Unlock()
		return
	}

	for _, t := range all {
		if r, ok := e.tableCache[t.FileNum]; ok {
			r.Close()
			delete(e.tableCache, t.FileNum)
		}
		os.Remove(e.tablePath(int(t.Level), t.FileNum))
	}
	e.mu.Unlock()

	e.isCompacting.Store(false)
	e.scheduleCompaction()
}

func (e *Engine) writeCompactionOutput(
	fileNum uint64,
	level int,
	iters []*sstable.SSTableIterator,
	isBottomLevel bool,
) ([]*meta.TableMeta, error) {
	merger := compaction.NewMergeIterator(iters)
	var outTables []*meta.TableMeta

	var writer *sstable.TableWriter
	var firstKey, lastKey record.InternalKey
	var lastUserKey []byte
	first := true

	openWriter := func() error {
		var err error
		e.ensureLevelDir(level)
		writer, err = sstable.Create(
			e.tablePath(level, fileNum),
			e.opts.BlockSize,
			e.opts.FilterExpectedKeys,
			e.opts.FilterFPRate,
		)
		first = true
		return err
	}

	closeWriter := func() error {
		if writer == nil {
			return nil
		}
		size, err := writer.Close()
		if err != nil {
			return err
		}

		outTables = append(outTables, &meta.TableMeta{
			FileNum:  fileNum,
			FileSize: uint64(size),
			Level:    uint32(level),
			MinKey:   firstKey,
			MaxKey:   lastKey,
		})

		writer = nil
		fileNum = e.vs.NextFileNum()
		return nil
	}

	if err := openWriter(); err != nil {
		return nil, err
	}

	oldestSnap := e.snapshots.oldest()
	for merger.Valid() {
		rec := merger.Record()

		if bytes.Equal(rec.UserKey, lastUserKey) {
			merger.Next()
			continue
		}
		lastUserKey = rec.UserKey

		if rec.Type == record.TypeDel && isBottomLevel && rec.SeqNum < oldestSnap {
			merger.Next()
			continue
		}

		if first {
			firstKey = rec.InternalKey
			first = false
		}
		lastKey = rec.InternalKey

		if err := writer.Add(rec); err != nil {
			return nil, err
		}

		size, _ := writer.EstimatedSize()
		if size >= e.opts.MaxSSTableSize {
			if err := closeWriter(); err != nil {
				return nil, err
			}

			if merger.Valid() {
				if err := openWriter(); err != nil {
					return nil, err
				}
			}
		}

		merger.Next()
	}

	if writer != nil {
		if !first {
			if err := closeWriter(); err != nil {
				return nil, err
			}
		} else {
			path := writer.Path()
			writer.Close()
			os.Remove(path)
		}
	}

	return outTables, nil
}

func (e *Engine) pickCompactionInputs(level int) []*meta.TableMeta {
	tables := e.vs.Current.Levels[level]
	if len(tables) == 0 {
		return nil
	}

	if level == 0 {
		return tables
	}

	pointer := e.compactPointer[level]
	idx := sort.Search(len(tables), func(i int) bool {
		return bytes.Compare(tables[i].MinKey.UserKey, pointer) > 0
	})

	var selected []*meta.TableMeta
	var pickedSize int64
	targetSize := max(e.levelSize(level)-e.levelCapacity(level), 0)

	for i := idx; i < len(tables); i++ {
		selected = append(selected, tables[i])
		pickedSize += int64(tables[i].FileSize)
		if pickedSize >= targetSize {
			break
		}
	}

	return selected
}

func (e *Engine) levelSize(level int) int64 {
	var total int64
	for _, t := range e.vs.Current.Levels[level] {
		total += int64(t.FileSize)
	}
	return total
}

func (e *Engine) levelCapacity(level int) int64 {
	cap := e.opts.MaxBytesBase

	for i := 1; i < level; i++ {
		cap *= int64(e.opts.LevelMultiplier)
	}
	return cap
}
