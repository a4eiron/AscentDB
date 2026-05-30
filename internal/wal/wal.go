package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"

	"github.com/a4eiron/ascentdb/internal/config"
	"github.com/a4eiron/ascentdb/internal/record"
)

type WAL struct {
	file *os.File
	mu   sync.Mutex
	opts config.SyncOptions

	commitCh chan commitReq
	closeCh  chan struct{}

	syncCh chan struct{}
}

type commitReq struct {
	buf  []byte
	done chan error
}

func Open(path string, opts config.SyncOptions) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	wal := &WAL{
		file:    file,
		opts:    opts,
		closeCh: make(chan struct{}),
		syncCh:  make(chan struct{}),
	}

	switch opts.Mode {
	case config.SyncNone:
		if opts.Interval > 0 {
			go wal.intervalSyncer()
		}
	case config.SyncBatch:
		wal.commitCh = make(chan commitReq, 256)
		go wal.committer()
	}

	return wal, nil
}

func (w *WAL) Append(r record.Record) error {
	payloadSize := int(r.Size())

	buf := make([]byte, 4+payloadSize+4)
	binary.LittleEndian.PutUint32(buf[:4], uint32(payloadSize))

	payload := buf[4 : 4+payloadSize]
	record.EncodeRecordInto(payload, r)
	checksum := crc32.ChecksumIEEE(payload)
	binary.LittleEndian.PutUint32(buf[4+payloadSize:], checksum)

	switch w.opts.Mode {
	case config.SyncBatch:
		return w.appendBatch(buf)
	default:
		return w.appendAsync(buf)
	}

}

func (w *WAL) appendAsync(buf []byte) error {
	w.mu.Lock()
	_, err := w.file.Write(buf)
	w.mu.Unlock()
	return err
}

func (w *WAL) appendBatch(buf []byte) error {
	done := make(chan error, 1)
	w.commitCh <- commitReq{buf: buf, done: done}
	return <-done
}

func Replay(w *WAL, fn func(r record.Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	sizeHeader := make([]byte, 4)
	var recBuf []byte
	checkSumBuf := make([]byte, 4)

	for {
		_, err := io.ReadFull(w.file, sizeHeader)

		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			return errors.New("wal: truncated header")
		}
		if err != nil {
			return err
		}

		recSize := binary.LittleEndian.Uint32(sizeHeader)
		if cap(recBuf) < int(recSize) {
			recBuf = make([]byte, recSize)
		}

		recBuf = recBuf[:recSize]

		if _, err := io.ReadFull(w.file, recBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return errors.New("wal: truncated payload")
			}
			return err
		}

		if _, err := io.ReadFull(w.file, checkSumBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return errors.New("wal: truncated checksum")
			}
			return err
		}

		expectedCRC := binary.LittleEndian.Uint32(checkSumBuf)
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
	if w == nil {
		return ""
	}
	return w.file.Name()
}

func (w *WAL) Close() error {
	if w == nil {
		return nil
	}

	close(w.closeCh)

	w.file.Sync()
	return w.file.Close()
}

func (w *WAL) committer() {
	for {
		var reqs []commitReq
		select {
		case req := <-w.commitCh:
			reqs = append(reqs, req)
		case <-w.closeCh:
			for {
				select {
				case req := <-w.commitCh:
					req.done <- errors.New("wal: closed")
				default:
					return
				}
			}
		}

		for {
			select {
			case req := <-w.commitCh:
				reqs = append(reqs, req)
			default:
				goto flush
			}
		}

	flush:
		var batch []byte
		for _, r := range reqs {
			batch = append(batch, r.buf...)
		}

		_, err := w.file.Write(batch)
		if err == nil {
			err = w.file.Sync()
		}

		for _, r := range reqs {
			r.done <- err
		}
	}
}

func (w *WAL) intervalSyncer() {
	ticker := time.NewTicker(w.opts.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			w.file.Sync()
			old := w.syncCh
			w.syncCh = make(chan struct{})
			w.mu.Unlock()
			close(old)

		case <-w.closeCh:
			return
		}
	}
}
