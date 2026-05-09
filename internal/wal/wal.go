package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"log"
	"os"
	"sync"

	"github.com/a4eiron/ascentdb/internal/codec"
	"github.com/a4eiron/ascentdb/internal/record"
)

type WAL struct {
	file *os.File
	mu   sync.Mutex
}

func Open(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file: file,
	}, nil
}

func (w *WAL) Append(r *record.Record) error {
	recordBytes := record.EncodeRecord(r)

	crc := crc32.ChecksumIEEE(recordBytes)

	buf := codec.NewBuffer(4 + 4 + len(recordBytes))

	buf.WriteUint32(crc)
	buf.WriteUint32(uint32(len(recordBytes)))
	buf.WriteBytes(recordBytes)

	b := buf.Bytes()

	w.mu.Lock()
	n, err := w.file.Write(b)
	if err != nil {
		return err
	}
	w.mu.Unlock()

	log.Println(n, len(buf.Bytes()))

	return w.file.Sync()
}

func Replay(w *WAL, fn func(r *record.Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	for {

		crcBuf := make([]byte, 4)
		_, err := io.ReadFull(w.file, crcBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		expectedCRC := binary.LittleEndian.Uint32(crcBuf)

		sizeBuf := make([]byte, 4)
		_, err = io.ReadFull(w.file, sizeBuf)
		if err != nil {
			return err
		}

		recSize := binary.LittleEndian.Uint32(sizeBuf)

		recBuf := make([]byte, recSize)
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

func (w *WAL) Path() string {
	return w.file.Name()
}
