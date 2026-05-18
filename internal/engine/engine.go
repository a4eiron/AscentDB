package engine

import (
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

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

	mt   *memtable.Memtable
	immt *memtable.Memtable

	tableCache map[uint64]*sstable.TableReader

	vs *meta.VersionSet

	flushChan chan *flushTask

	isCompacting   atomic.Bool
	compactPointer [7]string

	seqNum  uint64 // for every put/delete
	fileNum uint64 // sstable file sequence

	mu sync.Mutex
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
	e.fileNum = e.vs.NextFileNum()
	e.tableCache = make(map[uint64]*sstable.TableReader)

	if e.opts.CrashRecovery {
		walPath := filepath.Join(opts.DataDir, "wal", fmt.Sprintf("wal-%06d", e.vs.LogNumber()))
		log.Println("Opening wal", walPath)
		time.Sleep(4 * time.Second)

		wal, err := wal.Open(walPath)
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

	// create a new record
	r := &record.Record{
		InternalKey: record.InternalKey{
			UserKey: key,
			Type:    record.TypePut,
			SeqNum:  atomic.AddUint64(&e.seqNum, 1),
		},
		Value: value,
	}

	e.mu.Lock()

	// first - append to WAL if CrashRecovery is enabled
	if e.opts.CrashRecovery {
		if err := e.wal.Append(r); err != nil {
			log.Fatal("wal:", err)
		}
	}

	// second - insert into the memtable
	e.mt.Put(r)

	// third - check if the memtable is full, add a new flush task
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
		e.flushChan <- task
	}
}

func (e *Engine) Get(key string) ([]byte, bool) {

	// create lookup key for searching in SSTs
	lookupKey := record.InternalKey{
		UserKey: key,
		SeqNum:  math.MaxUint64,
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	// first - check active memtable
	if val, found, deleted := e.mt.Get(key); found {
		if deleted {
			return nil, false
		}

		return val, true
	}

	// second - if not present in the active memtable, check the immutable memtable
	if e.immt != nil {
		if val, found, deleted := e.immt.Get(key); found {
			if deleted {
				return nil, false
			}
			return val, true
		}
	}

	// third - search in SSTs, starting from the newest in L0
	var best *record.Record

	for level := range e.vs.Current.Levels {
		tables := e.vs.Current.Levels[level]

		for i := len(tables) - 1; i >= 0; i-- {
			t := tables[i]
			if key < t.MinKey.UserKey || key > t.MaxKey.UserKey {
				continue
			}

			reader, err := e.getReader(t)
			if err != nil {
				continue
			}

			rec, ok, err := reader.Get(lookupKey)
			if err != nil || !ok {
				continue
			}

			if best == nil || rec.SeqNum > best.SeqNum {
				best = rec
			}
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

func (e *Engine) Delete(key string) {
	r := &record.Record{
		InternalKey: record.InternalKey{
			UserKey: key,
			Type:    record.TypeDel,
			SeqNum:  atomic.AddUint64(&e.seqNum, 1),
		},
		Value: nil,
	}

	e.mu.Lock()

	if e.opts.CrashRecovery {
		if err := e.wal.Append(r); err != nil {
			log.Fatal("wal:", err)
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
