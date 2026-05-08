package codec

import (
	"encoding/binary"
	"errors"
)

var order = binary.LittleEndian

type Buffer struct {
	data   []byte
	offset int
}

// retuns a buffer with length n
func NewBuffer(n int) *Buffer {
	return &Buffer{
		data: make([]byte, n),
	}
}

func NewBufferFromBytes(d []byte) *Buffer {
	return &Buffer{
		data:   d,
		offset: 0,
	}
}

func (b *Buffer) WriteUint32(v uint32) {
	order.PutUint32(b.data[b.offset:b.offset+4], v)
	b.offset += 4
}

func (b *Buffer) WriteUint64(v uint64) {
	order.PutUint64(b.data[b.offset:b.offset+8], v)
	b.offset += 8
}

func (b *Buffer) WriteUint8(v uint8) {
	b.data[b.offset] = v
	b.offset += 1
}

func (b *Buffer) WriteBytes(d []byte) {
	length := len(d)
	copy(b.data[b.offset:b.offset+length], d)
	b.offset += length
}

func (b *Buffer) ReadUint32() (uint32, error) {
	if b.offset+4 > len(b.data) {
		return 0, errors.New("buffer: read uint32 out of bounds")
	}

	v := order.Uint32(b.data[b.offset : b.offset+4])
	b.offset += 4
	return v, nil
}

func (b *Buffer) ReadUint64() (uint64, error) {
	if b.offset+8 > len(b.data) {
		return 0, errors.New("buffer: read uint64 out of bounds")
	}

	v := order.Uint64(b.data[b.offset : b.offset+8])
	b.offset += 8
	return v, nil
}

func (b *Buffer) ReadUint8() (uint8, error) {
	if b.offset+1 > len(b.data) {
		return 0, errors.New("buffer: read uint8 out of bounds")
	}

	v := b.data[b.offset]
	b.offset += 1

	return v, nil
}

func (b *Buffer) ReadBytes(n int) ([]byte, error) {
	if b.offset+n > len(b.data) {
		return nil, errors.New("buffer: read bytes out of bounds")
	}

	d := b.data[b.offset : b.offset+n]
	b.offset += n
	return d, nil
}

func (b *Buffer) SetOffset(offset int) error {
	if offset > len(b.data) {
		return errors.New("buffer: offset out of bounds")
	}
	b.offset = offset
	return nil
}

func (b *Buffer) Bytes() []byte {
	return b.data[:b.offset]
}
