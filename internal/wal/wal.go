package wal

import (
	"hash/crc32"
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
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
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

	buf := codec.NewBuffer(4 + len(recordBytes))

	buf.WriteUint32(crc)
	buf.WriteBytes(recordBytes)

	b := buf.Bytes()

	w.mu.Lock()
	n, err := w.file.Write(b)
	if err != nil {
		return err
	}
	w.mu.Unlock()

	log.Println(n, len(buf.Bytes()))
	return nil
}
