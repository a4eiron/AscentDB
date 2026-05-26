package record

import (
	"encoding/binary"
	"errors"
)

var (
	ErrCorruptRecord = errors.New("record: corrupt record")
)

type Record struct {
	InternalKey
	Value []byte
}

func (r *Record) IsTombstone() bool {
	return r.Type == TypeDel
}

func (r Record) Size() uint32 {
	return r.KeySize() + 4 + uint32(len(r.Value))
}

func (r *Record) KeyLen() uint32 {
	return r.keyLen()
}

func (r *Record) ValueLen() uint32 {
	return uint32(len(r.Value))
}

func EncodeRecord(r Record) []byte {
	buf := make([]byte, r.Size())
	EncodeRecordInto(buf, r)
	return buf
}

func EncodeRecordInto(dst []byte, r Record) int {
	off := 0

	binary.LittleEndian.PutUint32(dst[off:off+4], uint32(len(r.UserKey)))
	off += 4

	copy(dst[off:], r.UserKey)
	off += len(r.UserKey)

	binary.LittleEndian.PutUint64(dst[off:off+8], r.SeqNum)
	off += 8

	dst[off] = byte(r.Type)
	off++

	binary.LittleEndian.PutUint32(dst[off:off+4], uint32(len(r.Value)))
	off += 4

	copy(dst[off:], r.Value)
	off += len(r.Value)

	return off
}

func DecodeRecord(data []byte) (Record, error) {
	var off int

	if len(data) < 4 {
		return Record{}, ErrCorruptRecord
	}

	keyLen := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4

	if off+keyLen > len(data) {
		return Record{}, ErrCorruptRecord
	}

	userKey := data[off : off+keyLen]
	off += keyLen

	if off+8 > len(data) {
		return Record{}, ErrCorruptRecord
	}

	seq := binary.LittleEndian.Uint64(data[off:])
	off += 8

	if off+1 > len(data) {
		return Record{}, ErrCorruptRecord
	}

	typ := IKType(data[off])
	off++

	if off+4 > len(data) {
		return Record{}, ErrCorruptRecord
	}

	valLen := int(binary.LittleEndian.Uint32(data[off:]))
	off += 4

	if off+valLen > len(data) {
		return Record{}, ErrCorruptRecord
	}

	value := data[off : off+valLen]

	return Record{
		InternalKey: InternalKey{
			UserKey: userKey,
			SeqNum:  seq,
			Type:    typ,
		},
		Value: value,
	}, nil
}
