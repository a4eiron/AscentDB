package engine

import (
	"fmt"
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
	compactPointer [][]byte

	snapshots SnapshotList

	mu sync.RWMutex
}

func New(opts *config.Options) (*Engine, error) {
	opts = config.WithDefaults(opts)

	if err := mkdirs(opts); err != nil {
		return nil, err
	}

	vs, err := meta.Open(opts.DataDir)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		opts:           opts,
		vs:             vs,
		mt:             memtable.New(uint64(opts.MemtableSize), opts.SkiplistMaxLevel, opts.SkiplistP),
		flushChan:      make(chan *flushTask, 6),
		blockCache:     sstable.NewBlockCache(opts.BlockCacheSize),
		tableCache:     make(map[uint64]*sstable.TableReader),
		compactPointer: make([][]byte, opts.NumLevels),
	}

	e.seqNum.Store(vs.LastSequenceNum())

	go e.runFlusher()

	if opts.CrashRecovery {
		if err := e.recover(); err != nil {
			return nil, fmt.Errorf("recovery: %v", err)
		}

		e.wal, err = wal.Open(e.walPath(e.vs.NextFileNum()), opts.SyncOptions)
		if err != nil {
			return nil, fmt.Errorf("open wal: %v", err)
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

	e.tableCacheMu.Lock()
	for _, r := range e.tableCache {
		if err := r.Close(); err != nil {
			return err
		}
	}
	e.tableCacheMu.Unlock()

	if e.wal != nil {
		return e.wal.Close()
	}
	return nil
}

func mkdirs(opts *config.Options) error {
	dirs := []string{
		filepath.Join(opts.DataDir, "wal"),
		filepath.Join(opts.DataDir, "tables"),
	}
	for level := range opts.NumLevels {
		dirs = append(dirs, filepath.Join(opts.DataDir, "tables", fmt.Sprintf("L%d", level)))
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return nil
}
