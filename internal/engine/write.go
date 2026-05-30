package engine

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/a4eiron/ascentdb/internal/record"
)

func (e *Engine) Put(key, value []byte) error {
	if bytes.Equal(key, nil) || bytes.Equal(value, nil) {
		return fmt.Errorf("cannot put with empty key or value")
	}
	return e.write(key, value, record.TypePut)
}

func (e *Engine) Delete(key []byte) {
	e.write(key, nil, record.TypeDel)
}

func (e *Engine) write(key, value []byte, typ record.IKType) error {
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

	if e.opts.CrashRecovery {
		if err := e.wal.Append(r); err != nil {
			return err
		}
	}

	e.mt.Put(r)

	e.mu.Lock()
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

func (e *Engine) recoveryWrite(r record.Record) error {

	e.mt.Put(r)
	var task *flushTask
	if e.mt.IsFull() {
		var err error
		task, err = e.rotate()
		if err != nil {
			return err
		}
	}
	if task != nil {
		e.flushWg.Add(1)
		e.flushChan <- task
	}

	return nil
}
