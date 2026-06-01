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

	commitCh chan *commitReq
	commitWg sync.WaitGroup

	closeCh chan struct{}
	closed  bool
}

type commitReq struct {
	buf []byte
	err error
	wg  sync.WaitGroup
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
	}

	switch opts.Mode {
	case config.SyncNone:
		if opts.Interval > 0 {
			go wal.intervalSyncer()
		}
	case config.SyncBatch:
		wal.commitCh = make(chan *commitReq, 256)
		wal.commitWg.Add(1)
		go wal.committer()
	}

	return wal, nil
}

func (w *WAL) Append(r record.Record) error {
	return w.submit(encodeRecords([]record.Record{r}))
}

func (w *WAL) AppendBatch(recs []record.Record, startSeq uint64) error {
	for i := range recs {
		recs[i].SeqNum = startSeq + uint64(i)
	}
	return w.submit(encodeRecords(recs))
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

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true

	if w.commitCh != nil {
		close(w.commitCh)
	}
	close(w.closeCh)
	w.mu.Unlock()
	w.commitWg.Wait()

	if err := w.file.Sync(); err != nil {
		return err
	}
	return w.file.Close()
}

func (w *WAL) committer() {
	defer w.commitWg.Done()
	for {
		req, ok := <-w.commitCh
		if !ok {
			return
		}

		reqs := []*commitReq{req}
		time.Sleep(5 * time.Millisecond)

	drain:
		for {
			select {
			case r, ok := <-w.commitCh:
				if !ok {
					break drain
				}
				reqs = append(reqs, r)

			default:
				break drain
			}
		}
		total := 0

		for _, r := range reqs {
			total += len(r.buf)
		}

		batch := make([]byte, 0, total)
		for _, r := range reqs {
			batch = append(batch, r.buf...)
		}

		_, err := w.file.Write(batch)
		if err == nil {
			err = w.file.Sync()
		}

		for _, r := range reqs {
			r.err = err
			r.wg.Done()
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
			w.mu.Unlock()

		case <-w.closeCh:
			return
		}
	}
}

func (w *WAL) submit(buf []byte) error {
	switch w.opts.Mode {
	case config.SyncBatch:
		req := &commitReq{buf: buf}
		req.wg.Add(1)

		w.mu.Lock()
		if w.closed {
			w.mu.Unlock()
			return errors.New("wal: closed")
		}
		w.mu.Unlock()

		w.commitCh <- req
		req.wg.Wait()
		return req.err
	default:
		w.mu.Lock()
		defer w.mu.Unlock()

		if w.closed {
			return errors.New("wal: closed")
		}

		_, err := w.file.Write(buf)
		return err
	}
}

func encodeRecords(recs []record.Record) []byte {
	var total int

	for _, rec := range recs {
		total += 4 + int(rec.Size()) + 4
	}

	buf := make([]byte, total)
	off := 0

	for _, rec := range recs {
		size := int(rec.Size())

		binary.LittleEndian.PutUint32(buf[off:], uint32(size))
		off += 4

		record.EncodeRecordInto(buf[off:off+size], rec)
		crc := crc32.ChecksumIEEE(buf[off : off+size])
		off += size

		binary.LittleEndian.PutUint32(buf[off:], crc)
		off += 4
	}
	return buf
}
