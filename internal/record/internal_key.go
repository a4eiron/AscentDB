package record

import "github.com/a4eiron/ascentdb/internal/codec"

type IKType uint8

const TypePut IKType = 1
const TypeDel IKType = 0

type InternalKey struct {
	UserKey string
	SeqNum  uint64
	Type    IKType
}

func (k *InternalKey) KeySize() uint32 {
	return 4 + uint32(len(k.UserKey)) + 8 + 1
}

func (k *InternalKey) keyLen() uint32 {
	return uint32(len(k.UserKey))
}

func (k *InternalKey) Compare(other InternalKey) int {

	if k.UserKey < other.UserKey {
		return -1
	}

	if k.UserKey > other.UserKey {
		return 1
	}

	if k.SeqNum < other.SeqNum {
		return 1
	}

	if k.SeqNum > other.SeqNum {
		return -1
	}

	return 0
}

func EncodeInternalKey(buf *codec.Buffer, k *InternalKey) {
	buf.WriteUint32(k.keyLen())
	buf.WriteBytes([]byte(k.UserKey))
	buf.WriteUint64(k.SeqNum)
	buf.WriteUint8(uint8(k.Type))
}

func DecodeInternalKey(buf *codec.Buffer) (*InternalKey, error) {
	keyLen, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	keyBytes, err := buf.ReadBytes(int(keyLen))
	if err != nil {
		return nil, err
	}

	seq, err := buf.ReadUint64()
	if err != nil {
		return nil, err
	}

	t, err := buf.ReadUint8()
	if err != nil {
		return nil, err
	}

	return &InternalKey{
		UserKey: string(keyBytes),
		SeqNum:  seq,
		Type:    IKType(t),
	}, nil
}
