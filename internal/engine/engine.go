package engine

import (
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/a4eiron/ascentdb/internal"
	"github.com/a4eiron/ascentdb/internal/config"
	"github.com/a4eiron/ascentdb/internal/memtable"
	"github.com/a4eiron/ascentdb/internal/meta"
	"github.com/a4eiron/ascentdb/internal/record"
	"github.com/a4eiron/ascentdb/internal/sstable"
	"github.com/a4eiron/ascentdb/internal/wal"
)

type Engine struct {
	opts *config.Options

	wal   *wal.WAL
	imwal *wal.WAL

	mt        *memtable.Memtable
	immt      *memtable.Memtable
	immtQueue []*memtable.Memtable

	tableCache map[uint64]*sstable.TableReader

	vs *meta.VersionSet

	flushChan chan *flushTask
	flushWg   sync.WaitGroup

	isCompacting   atomic.Bool
	compactPointer [7]string

	seqNum uint64 // for every put/delete

	snapshots SnapshotList

	mu sync.RWMutex
}

func New(opts *config.Options) (*Engine, error) {

	err := os.MkdirAll(filepath.Join(opts.DataDir, "tables"), 0755)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(filepath.Join(opts.DataDir, "wal"), 0755)
	if err != nil {
		return nil, err
	}

	for i := range 7 {
		if err := os.MkdirAll(filepath.Join(opts.DataDir, "tables", fmt.Sprintf("L%d", i)), 0755); err != nil {
			return nil, err
		}
	}

	e := &Engine{
		opts:      opts,
		mt:        memtable.New(uint64(opts.MemtableSize)),
		flushChan: make(chan *flushTask, 6),
	}

	vs, err := meta.Open(opts.DataDir)
	if err != nil {
		return nil, err
	}

	e.vs = vs
	atomic.StoreUint64(&e.seqNum, e.vs.LastSequenceNum())
	e.tableCache = make(map[uint64]*sstable.TableReader)

	if e.opts.CrashRecovery {
		walPath := filepath.Join(opts.DataDir, "wal", fmt.Sprintf("wal-%06d.log", e.vs.LogNumber()))
		wal, err := wal.Open(walPath, e.opts.WALSyncInterval)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		e.wal = wal

		if err := e.recover(); err != nil {
			log.Println(err)
			return nil, err
		}
	}

	// background flusher
	go e.runFlusher()
	return e, nil
}

func (e *Engine) Put(key string, value []byte) {
	e.write(key, value, record.TypePut)
}

func (e *Engine) Get(key string) ([]byte, bool) {
	return e.getAt(key, math.MaxUint64)
}

func (e *Engine) Delete(key string) {
	e.write(key, nil, record.TypeDel)
}

func (e *Engine) getAt(key string, seqNum uint64) ([]byte, bool) {
	lookupKey := record.InternalKey{
		UserKey: key,
		SeqNum:  seqNum,
	}

	e.mu.RLock()
	defer e.mu.RUnlock()

	if val, found, deleted := e.mt.Get(key, lookupKey); found {
		if deleted {
			return nil, false
		}
		return val, true
	}

	if e.immt != nil {
		if val, found, deleted := e.immt.Get(key, lookupKey); found {
			if deleted {
				return nil, false
			}
			return val, true
		}
	}

	var best *record.Record

	checkTable := func(t *meta.TableMeta) {
		reader, err := e.getReader(t)
		if err != nil {
			return
		}

		rec, ok, err := reader.Get(lookupKey)
		if err != nil || !ok {
			return
		}

		if best == nil || rec.SeqNum > best.SeqNum {
			best = rec
		}
	}

	for level := range e.vs.Current.Levels {
		tables := e.vs.Current.Levels[level]
		if level == 0 {
			for i := len(tables) - 1; i >= 0; i-- {
				t := tables[i]
				// log.Println("Searching file:", t.FileNum)
				if key < t.MinKey.UserKey || key > t.MaxKey.UserKey {
					continue
				}
				checkTable(t)
			}
		} else {
			if len(tables) == 0 {
				continue
			}

			idx := sort.Search(len(tables), func(i int) bool {
				return tables[i].MaxKey.Compare(lookupKey) >= 0
			})

			if idx >= len(tables) {
				continue
			}
			checkTable(tables[idx])
		}

	}

	if best != nil {
		if best.Type == record.TypeDel {
			return nil, false
		}
		return best.Value, true
	}

	return nil, false
}

func (e *Engine) Scan(start, end string) *ScanIterator {
	e.mu.RLock()
	defer e.mu.RUnlock()

	var iters []internal.Iterator

	mtIter := e.mt.Iterator()
	mtIter.Seek(record.InternalKey{UserKey: start, SeqNum: math.MaxUint64})
	iters = append(iters, mtIter)

	if e.immt != nil {
		immtIter := e.immt.Iterator()
		immtIter.Seek(record.InternalKey{UserKey: start, SeqNum: math.MaxUint64})
		iters = append(iters, immtIter)
	}

	for _, tables := range e.vs.Current.Levels {
		for _, t := range tables {
			if t.MaxKey.UserKey < start || t.MinKey.UserKey > end {
				continue
			}

			reader, err := e.getReader(t)
			if err != nil {
				log.Fatalln(err)
			}

			iter := reader.Iterator()
			iter.Seek(record.InternalKey{UserKey: start, SeqNum: math.MaxUint64})
			iters = append(iters, iter)
		}
	}

	return NewScanIterator(iters, end)
}

func (e *Engine) Sync() error {
	if e.opts.CrashRecovery && e.wal != nil {
		e.mu.Lock()
		err := e.wal.Sync()
		e.mu.Unlock()
		if err != nil {
			return err
		}
	}

	e.mu.Lock()
	if e.mt.Size() == 0 {
		e.mu.Unlock()
		return nil
	}
	task, err := e.rotate()
	if err != nil {
		e.mu.Unlock()
		return err
	}
	e.mu.Unlock()

	e.flushWg.Add(1)
	e.flushChan <- task
	e.flushWg.Wait()

	return nil
}

func (e *Engine) Close() error {
	var task *flushTask
	e.mu.Lock()
	if e.mt.Size() > 0 {
		var err error
		task, err = e.rotate()
		if err != nil {
			e.mu.Unlock()
			return err
		}
	}
	e.mu.Unlock()

	if task != nil {
		e.flushWg.Add(1)
		e.flushChan <- task
	}

	e.flushWg.Wait()

	close(e.flushChan)

	for _, r := range e.tableCache {
		r.Close()
	}

	if e.wal != nil {
		return e.wal.Close()
	}
	return nil
}

func (e *Engine) write(key string, value []byte, typ record.IKType) {
	seq := atomic.AddUint64(&e.seqNum, 1)
	r := &record.Record{
		InternalKey: &record.InternalKey{
			UserKey: key,
			Type:    typ,
			SeqNum:  seq,
		},
		Value: value,
	}

	e.mu.Lock()

	if e.opts.CrashRecovery {
		if err := e.wal.Append(r); err != nil {
			log.Fatal("wal", err)
		}
	}

	e.mt.Put(r)

	var task *flushTask
	if e.mt.IsFull() {
		var err error
		task, err = e.rotate()
		if err != nil {
			log.Println(err)
		}
	}
	e.mu.Unlock()

	if task != nil {
		e.flushWg.Add(1)
		e.flushChan <- task
	}

}

func (e *Engine) getReader(t *meta.TableMeta) (*sstable.TableReader, error) {
	if r, ok := e.tableCache[t.FileNum]; ok {
		return r, nil
	}

	path := e.tablePath(int(t.Level), t.FileNum)
	r, err := sstable.Open(path)
	if err != nil {
		return nil, err
	}

	e.tableCache[t.FileNum] = r
	return r, nil
}
