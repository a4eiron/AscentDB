package engine

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/a4eiron/ascentdb/internal/config"
	"github.com/a4eiron/ascentdb/internal/memtable"
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

	sstables []*sstable.TableReader

	flushChan chan *flushTask
	flushing  bool

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

	e := &Engine{
		opts:      opts,
		mt:        memtable.New(64 * 1024),
		flushChan: make(chan *flushTask, 1),
	}

	if e.opts.CrashRecovery {
		wal, err := wal.Open(filepath.Join(opts.DataDir, "wal", fmt.Sprintf("wal-%06d", e.fileNum)))
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
	e.mu.Lock()
	defer e.mu.Unlock()

	r := &record.Record{
		InternalKey: record.InternalKey{
			UserKey: key,
			Type:    record.TypePut,
			SeqNum:  atomic.AddUint64(&e.seqNum, 1),
		},
		Value: value,
	}

	if e.opts.CrashRecovery {
		if err := e.wal.Append(r); err != nil {
			log.Fatal("wal:", err)
		}
	}
	e.mt.Put(r)

	if e.mt.IsFull() {
		e.rotate()
	}
}

func (e *Engine) Get(key string) ([]byte, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// first check active memtable
	if val, ok := e.mt.Get(key); ok {
		return val, ok
	}

	// if not present in the active memtable, check the immutable memtable
	if val, ok := e.immt.Get(key); ok {
		return val, ok
	}

	// lookupKey := record.InternalKey{
	// 	UserKey: key,
	// 	SeqNum:  math.MaxUint64,
	// }

	// TODO: sstable check
	return nil, false
}

func (e *Engine) Delete(key string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	r := &record.Record{
		InternalKey: record.InternalKey{
			UserKey: key,
			Type:    record.TypeDel,
			SeqNum:  atomic.AddUint64(&e.seqNum, 1),
		},
		Value: nil,
	}

	if e.opts.CrashRecovery {
		if err := e.wal.Append(r); err != nil {
			log.Fatal("wal:", err)
		}
	}
	e.mt.Put(r)

	if e.mt.IsFull() {
		e.rotate()
	}
}
