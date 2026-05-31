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
	"github.com/a4eiron/ascentdb/internal/meta"
	"github.com/a4eiron/ascentdb/internal/sstable"
	"github.com/a4eiron/ascentdb/internal/wal"
)

type Engine struct {
	opts *config.Options

	wal   *wal.WAL
	imwal *wal.WAL

	mt   *memtable.Memtable
	immt *memtable.Memtable

	vs     *meta.VersionSet
	seqNum atomic.Uint64

	tableCache   map[uint64]*sstable.TableReader
	tableCacheMu sync.RWMutex

	blockCache *sstable.BlockCache

	flushChan chan *flushTask
	flushWg   sync.WaitGroup
	compactWg sync.WaitGroup

	isCompacting   atomic.Bool
	compactPointer [7][]byte

	snapshots SnapshotList

	mu sync.RWMutex
}

func New(opts *config.Options) (*Engine, error) {
	if err := os.MkdirAll(filepath.Join(opts.DataDir, "tables"), 0755); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(filepath.Join(opts.DataDir, "wal"), 0755); err != nil {
		return nil, err
	}

	for level := range 7 {
		dir := filepath.Join(opts.DataDir, "tables", fmt.Sprintf("L%d", level))
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, err
		}
	}

	e := &Engine{
		opts:       opts,
		mt:         memtable.New(uint64(opts.MemtableSize)),
		flushChan:  make(chan *flushTask, 6),
		blockCache: sstable.NewBlockCache(1024),
		tableCache: make(map[uint64]*sstable.TableReader),
	}

	vs, err := meta.Open(opts.DataDir)
	if err != nil {
		return nil, err
	}

	e.vs = vs
	e.seqNum.Store(vs.LastSequenceNum())

	go e.runFlusher()

	if opts.CrashRecovery {
		if err := e.recover(); err != nil {
			log.Println(err)
			return nil, err
		}

		walPath := filepath.Join(
			opts.DataDir,
			"wal",
			fmt.Sprintf("wal-%06d.log", e.vs.NextFileNum()),
		)

		e.wal, err = wal.Open(walPath, opts.SyncOptions)
		if err != nil {
			log.Println(err)
			return nil, err
		}

	}

	return e, nil
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
	e.compactWg.Wait()

	for _, r := range e.tableCache {
		if err := r.Close(); err != nil {
			return err
		}
	}

	if e.wal != nil {
		return e.wal.Close()
	}
	return nil
}
