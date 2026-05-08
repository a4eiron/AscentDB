package record

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
