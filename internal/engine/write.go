package engine

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/a4eiron/ascentdb/internal/record"
)

func (e *Engine) Put(key string, value []byte) error {
	if key == "" || value == nil {
		return fmt.Errorf("cannot put with empty key or value")
	}
	return e.write(key, value, record.TypePut)
}

func (e *Engine) Delete(key string) {
	e.write(key, nil, record.TypeDel)
}

func (e *Engine) write(key string, value []byte, typ record.IKType) error {
	seq := atomic.AddUint64(&e.seqNum, 1)
	r := record.Record{
		InternalKey: record.InternalKey{
			UserKey: key,
			Type:    typ,
			SeqNum:  seq,
		},
		Value: value,
	}

	for {

		e.mu.RLock()
		l0Count := len(e.vs.Current.Levels[0])
		e.mu.RUnlock()
		if l0Count < maxL0Files*2 {
			break
		}
		time.Sleep(1 * time.Second)
	}
	e.mu.Lock()

	if e.opts.CrashRecovery {
		if err := e.wal.Append(r); err != nil {
			return err
		}
	}

	e.mt.Put(r)

	var task *flushTask
	if e.mt.IsFull() {
		var err error
		task, err = e.rotate()
		if err != nil {
			return err
		}
	}
	e.mu.Unlock()

	if task != nil {
		e.flushWg.Add(1)
		e.flushChan <- task
	}

	return nil
}
