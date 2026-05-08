package sstable

import (
	"errors"

	"github.com/a4eiron/ascentdb/internal/codec"
)

const Magic uint64 = 0xdbb5a3c4f1e2d678

const FooterSize int = 8 + 4 + 8

type Footer struct {
	IndexOffset uint64
	IndexSize   uint32
	Magic       uint64
}

func encodeFooter(f Footer) []byte {
	buf := codec.NewBuffer(FooterSize)

	buf.WriteUint64(f.IndexOffset)
	buf.WriteUint32(f.IndexSize)
	buf.WriteUint64(f.Magic)

	return buf.Bytes()
}

func decodeFooter(b []byte) (*Footer, error) {

	buf := codec.NewBufferFromBytes(b)

	indexOffset, err := buf.ReadUint64()
	if err != nil {
		return nil, err
	}

	indexSize, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	magic, err := buf.ReadUint64()
	if err != nil {
		return nil, err
	}

	if magic != Magic {
		return nil, errors.New("footer: corrupt sstable")
	}

	f := &Footer{
		IndexOffset: indexOffset,
		IndexSize:   indexSize,
		Magic:       magic,
	}

	return f, nil
}
