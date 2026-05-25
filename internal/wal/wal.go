package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"

	"github.com/a4eiron/ascentdb/internal/codec"
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
	recordBytes := record.EncodeRecord(r)
	crc := crc32.ChecksumIEEE(recordBytes)

	buf := codec.NewBuffer(4 + 4 + len(recordBytes))
	buf.WriteUint32(crc)
	buf.WriteUint32(uint32(len(recordBytes)))
	buf.WriteBytes(recordBytes)

	w.mu.Lock()
	_, err := w.file.Write(buf.Bytes())
	w.mu.Unlock()
	return err
}

func Replay(w *WAL, fn func(r record.Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	crcBuf := make([]byte, 4)
	sizeBuf := make([]byte, 4)
	var recBuf []byte

	for {

		_, err := io.ReadFull(w.file, crcBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		expectedCRC := binary.LittleEndian.Uint32(crcBuf)

		_, err = io.ReadFull(w.file, sizeBuf)
		if err != nil {
			return err
		}

		recSize := binary.LittleEndian.Uint32(sizeBuf)
		if cap(recBuf) < int(recSize) {
			recBuf = make([]byte, recSize)
		}
		_, err = io.ReadFull(w.file, recBuf)
		if err != nil {
			return err
		}

		actualCRC := crc32.ChecksumIEEE(recBuf)
		if actualCRC != expectedCRC {
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
