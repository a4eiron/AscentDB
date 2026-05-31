package engine

import (
	"bytes"
	"fmt"
	"time"

	"github.com/a4eiron/ascentdb/internal/batch"
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
	seq := e.seqNum.Add(1)
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

	e.mu.Lock()
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

func (e *Engine) WriteBatch(b *batch.Batch) error {
	if b.Len() == 0 {
		return nil
	}

	for _, rec := range b.Records() {
		if rec.KeyLen() == 0 {
			return fmt.Errorf("writebatch: empty key")
		}

		if rec.Type == record.TypePut && rec.Value == nil {
			return fmt.Errorf("writebatch: nil value for key %s", rec.UserKey)
		}
	}

	n := uint64(b.Len())
	endSeq := e.seqNum.Add(n)
	startSeq := endSeq - n + 1

	recs := make([]record.Record, b.Len())
	for i, rec := range b.Records() {
		rec.SeqNum = startSeq + uint64(i)
		recs[i] = rec
	}

	if e.opts.CrashRecovery {
		if err := e.wal.AppendBatch(recs, startSeq); err != nil {
			return err
		}
	}

	e.mu.Lock()
	var tasks []*flushTask

	for _, r := range recs {
		e.mt.Put(r)
		if e.mt.IsFull() {
			task, err := e.rotate()
			if err != nil {
				e.mu.Unlock()
				return err
			}

			if task != nil {
				tasks = append(tasks, task)
			}
		}
	}

	e.mu.Unlock()
	for _, task := range tasks {
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
