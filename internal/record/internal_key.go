package record

import (
	"bytes"
	"encoding/binary"
)

type IKType uint8

const TypePut IKType = 1
const TypeDel IKType = 0

type InternalKey struct {
	UserKey []byte
	SeqNum  uint64
	Type    IKType
}

func (k *InternalKey) KeySize() uint32 {
	return 4 + uint32(len(k.UserKey)) + 8 + 1
}

func (k *InternalKey) keyLen() uint32 {
	return uint32(len(k.UserKey))
}

func (k InternalKey) Compare(other InternalKey) int {
	if cmp := bytes.Compare(k.UserKey, other.UserKey); cmp != 0 {
		return cmp
	}
	if k.SeqNum < other.SeqNum {
		return 1
	}
	if k.SeqNum > other.SeqNum {
		return -1
	}
	return 0
}

func EncodeInternalKey(dst []byte, off int, k InternalKey) int {
	binary.LittleEndian.PutUint32(dst[off:], uint32(len(k.UserKey)))
	off += 4
	copy(dst[off:], k.UserKey)
	off += len(k.UserKey)
	binary.LittleEndian.PutUint64(dst[off:], k.SeqNum)
	off += 8
	dst[off] = byte(k.Type)
	off++
	return off
}

func DecodeInternalKey(src []byte, off int) (InternalKey, int, error) {
	if off+4 > len(src) {
		return InternalKey{}, off, ErrCorruptRecord
	}
	keyLen := int(binary.LittleEndian.Uint32(src[off:]))
	off += 4

	if off+keyLen > len(src) {
		return InternalKey{}, off, ErrCorruptRecord
	}
	userKey := src[off : off+keyLen]
	off += keyLen

	if off+8 > len(src) {
		return InternalKey{}, off, ErrCorruptRecord
	}
	seq := binary.LittleEndian.Uint64(src[off:])
	off += 8

	if off+1 > len(src) {
		return InternalKey{}, off, ErrCorruptRecord
	}
	typ := IKType(src[off])
	off++

	return InternalKey{UserKey: userKey, SeqNum: seq, Type: typ}, off, nil
}
