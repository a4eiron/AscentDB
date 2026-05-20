package engine

import (
	"log"
	"os"

	"github.com/a4eiron/ascentdb/internal/compaction"
	"github.com/a4eiron/ascentdb/internal/meta"
	"github.com/a4eiron/ascentdb/internal/record"
	"github.com/a4eiron/ascentdb/internal/sstable"
)

const (
	maxL0Files      = 4
	maxBytesBase    = 10 * 1024 * 1024 // L1 cap
	levelMultiplier = 10               // each level is 10x larger

	maxSSTableSize = 2 * 1024 * 1024 // 2 MiB
)

func (e *Engine) scheduleCompaction() {

	if !e.isCompacting.CompareAndSwap(false, true) {
		return
	}

	if len(e.vs.Current.Levels[0]) >= maxL0Files {
		inputs := e.pickCompactionInputs(0)
		outFileNum := e.vs.NextFileNum()
		go func() {
			e.compactLevel(0, inputs, outFileNum)
		}()
		return
	}

	for level := 1; level < len(e.vs.Current.Levels)-1; level++ {

		if e.levelSize(level) > e.levelCapacity(level) {
			inputs := e.pickCompactionInputs(level)
			outFileNum := e.vs.NextFileNum()
			go func() {
				e.compactLevel(level, inputs, outFileNum)
			}()
			return
		}

	}

	e.isCompacting.Store(false)
}

func (e *Engine) compactLevel(level int, inputs []*meta.TableMeta, outFileNum uint64) {
	nextLevel := level + 1

	// key range covered by inputs
	minKey, maxKey := meta.KeyRangeOf(inputs)

	// find overlaping files in the next level
	e.mu.Lock()
	nextLevelFiles := append([]*meta.TableMeta(nil), e.vs.Current.Levels[nextLevel]...)

	if level > 0 && len(inputs) > 0 {
		e.compactPointer[level] = inputs[len(inputs)-1].MaxKey.UserKey
	}
	e.mu.Unlock()

	overlapping := meta.FindOverlapping(nextLevelFiles, minKey, maxKey)

	// open iterators for all input files
	all := append(inputs, overlapping...)
	iters := make([]*sstable.Iterator, 0, len(all))

	for _, t := range all {
		path := e.tablePath(int(t.Level), t.FileNum)
		reader, err := sstable.Open(path)
		if err != nil {
			log.Println("compaction - open:", err)
			return
		}
		iters = append(iters, reader.Iterator())
	}

	isBottomLevel := nextLevel == len(e.vs.Current.Levels)-1

	outTables, err := e.writeCompactionOutput(outFileNum, nextLevel, iters, isBottomLevel)
	if err != nil {
		log.Println(err)
		return
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
	}

	for _, t := range all {
		delete(e.tableCache, t.FileNum)
		os.Remove(e.tablePath(int(t.Level), t.FileNum))
	}
	shouldCompact := len(e.vs.Current.Levels[0]) >= maxL0Files
	e.mu.Unlock()
	e.isCompacting.Store(false)
	if shouldCompact {
		e.scheduleCompaction()
	}
}

func (e *Engine) writeCompactionOutput(
	fileNum uint64,
	level int,
	iters []*sstable.Iterator,
	isBottomLevel bool,
) ([]*meta.TableMeta, error) {

	merger := compaction.NewMergeIterator(iters)
	var outTables []*meta.TableMeta

	var writer *sstable.TableWriter
	var firstKey, lastKey *record.InternalKey
	var lastUserKey string
	first := true

	openWriter := func() error {
		var err error
		e.ensureLevelDir(level)
		writer, err = sstable.Create(e.tablePath(level, fileNum), e.opts.BlockSize)
		first = true
		return err
	}

	closeWriter := func() error {
		if writer == nil {
			return nil
		}
		size, _ := writer.Size()
		if err := writer.Close(); err != nil {
			return err
		}

		outTables = append(outTables, &meta.TableMeta{
			FileNum:  fileNum,
			FileSize: uint64(size),
			Level:    uint32(level),
			MinKey:   *firstKey,
			MaxKey:   *lastKey,
		})

		writer = nil
		fileNum = e.vs.NextFileNum()
		return nil
	}

	if err := openWriter(); err != nil {
		return nil, err
	}

	for merger.Valid() {
		rec := merger.Record()

		// skip older veresions of the same user key
		if rec.UserKey == lastUserKey {
			merger.Next()
			continue
		}
		lastUserKey = rec.UserKey

		// drop tombstones at the bottom level
		if isBottomLevel && rec.Type == record.TypeDel {
			merger.Next()
			continue
		}

		if first {
			firstKey = rec.InternalKey
			first = false
		}
		lastKey = rec.InternalKey

		if err := writer.Add(*rec); err != nil {
			return nil, err
		}

		size, _ := writer.Size()
		if size >= maxSSTableSize {
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
			writer.Close()
		}
	}

	return outTables, nil
}

// pick one file from a given level
func (e *Engine) pickCompactionInputs(level int) []*meta.TableMeta {
	tables := e.vs.Current.Levels[level]
	if len(tables) == 0 {
		return nil
	}

	// L0 files can overlap with each other, so talking all of them
	if level == 0 {
		return tables
	}

	// for L1+ whose Minkey > compactPointer[level]
	pointer := e.compactPointer[level]
	idx := 0
	for i, t := range tables {
		if t.MinKey.UserKey > pointer {
			idx = i
			break
		}
	}

	return []*meta.TableMeta{tables[idx]}
}

func (e *Engine) levelSize(level int) int64 {
	var total int64

	for _, t := range e.vs.Current.Levels[level] {
		total += int64(t.FileSize)
	}
	return total
}

func (e *Engine) levelCapacity(level int) int64 {
	cap := int64(maxBytesBase)

	for i := 1; i < level; i++ {
		cap *= levelMultiplier
	}
	return cap
}
