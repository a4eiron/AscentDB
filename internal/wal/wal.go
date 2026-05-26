package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"

	"github.com/a4eiron/ascentdb/internal/record"
)

type WAL struct {
	file      *os.File
	mu        sync.Mutex
	syncChan  chan struct{}
	closeChan chan struct{}
	interval  time.Duration
}

func Open(path string, syncInternval time.Duration) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		file:      file,
		syncChan:  make(chan struct{}),
		closeChan: make(chan struct{}),
		interval:  syncInternval,
	}

	if syncInternval > 0 {
		go wal.syncer()
	}

	return wal, nil
}

func (w *WAL) syncer() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			w.file.Sync()

			old := w.syncChan
			w.syncChan = make(chan struct{})
			w.mu.Unlock()
			close(old)
		case <-w.closeChan:
			return
		}
	}

}

func (w *WAL) Append(r record.Record) error {
	payloadSize := int(r.Size())

	buf := make([]byte, 8+payloadSize)

	record.EncodeRecordInto(buf[8:], r)

	payload := buf[8:]

	binary.LittleEndian.PutUint32(
		buf[0:4],
		crc32.ChecksumIEEE(payload),
	)

	binary.LittleEndian.PutUint32(
		buf[4:8],
		uint32(payloadSize),
	)

	w.mu.Lock()
	_, err := w.file.Write(buf)
	w.mu.Unlock()

	return err
}

func Replay(w *WAL, fn func(r record.Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	header := make([]byte, 8)
	var recBuf []byte

	for {
		_, err := io.ReadFull(w.file, header)

		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			return errors.New("wal: truncated header")
		}
		if err != nil {
			return err
		}

		expectedCRC := binary.LittleEndian.Uint32(header[0:4])
		recSize := binary.LittleEndian.Uint32(header[4:8])

		if cap(recBuf) < int(recSize) {
			recBuf = make([]byte, recSize)
		}

		recBuf = recBuf[:recSize]

		if _, err := io.ReadFull(w.file, recBuf); err != nil {
			return err
		}

		if crc32.ChecksumIEEE(recBuf) != expectedCRC {
			return errors.New("wal: corrupt record")
		}

		rec, err := record.DecodeRecord(recBuf)
		if err != nil {
			return err
		}

		if err := fn(rec); err != nil {
			return err
		}
	}

	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Sync()
}

func (w *WAL) Path() string {
	return w.file.Name()
}

func (w *WAL) Close() error {
	if w == nil {
		return nil
	}
	if w.interval > 0 {
		close(w.closeChan)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.file.Sync()
	return w.file.Close()
}
