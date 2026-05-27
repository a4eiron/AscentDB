package engine

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"time"

	"github.com/a4eiron/ascentdb/internal/record"
	"github.com/a4eiron/ascentdb/internal/wal"
)

func (e *Engine) recover() error {

	walDir := filepath.Join(e.opts.DataDir, "wal")
	logNumber := e.vs.LogNumber()

	log.Println("lastlognumber:", logNumber)

	time.Sleep(4 * time.Second)

	entries, err := os.ReadDir(walDir)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		if e.wal, err = wal.Open(e.walPath(logNumber), e.opts.WALSyncInterval); err != nil {
			return err
		}
		return nil
	}

	var fileNums []uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		var num uint64
		if _, err := fmt.Sscanf(entry.Name(), "wal-%06d.log", &num); err != nil {
			log.Println(err)
			continue
		}
		fileNums = append(fileNums, num)
	}

	slices.Sort(fileNums)

	for _, num := range fileNums {
		w, err := wal.Open(e.walPath(num), 0)
		if err != nil {
			return err
		}

		var maxSeq uint64
		if err := wal.Replay(w, func(r record.Record) error {
			log.Println("recovered userkey", string(r.UserKey))
			if err := e.recoveryWrite(r); err != nil {
				return err
			}
			if r.SeqNum > maxSeq {
				maxSeq = r.SeqNum
			}
			return nil
		}); err != nil {
			return err
		}

		err = w.Close()
		if err != nil {
			return err
		}

		if err := os.Remove(w.Path()); err != nil {
			return err
		}

		log.Println("recovered", num)

		if maxSeq > e.seqNum {
			atomic.StoreUint64(&e.seqNum, maxSeq)
		}
	}

	return nil
}
