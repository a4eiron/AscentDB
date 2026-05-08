package record

import "github.com/a4eiron/ascentdb/internal/codec"

type Record struct {
	InternalKey
	Value []byte
}

func (r *Record) IsTombstone() bool {
	return r.Type == TypeDel
}

func (r *Record) Size() uint32 {
	return r.KeySize() + 4 + uint32(len(r.Value))
}

func (r *Record) KeyLen() uint32 {
	return r.keyLen()
}

func (r *Record) ValueLen() uint32 {
	return uint32(len(r.Value))
}

func EncodeRecord(r *Record) []byte {
	payload := codec.NewBuffer(int(r.Size()))

	payload.WriteUint32(r.KeyLen())
	payload.WriteUint32(r.ValueLen())
	payload.WriteBytes([]byte(r.UserKey))
	payload.WriteUint64(uint64(r.SeqNum))
	payload.WriteUint8(uint8(r.Type))
	payload.WriteBytes(r.Value)

	return payload.Bytes()
}

func DecodeRecord(data []byte) (*Record, error) {
	buf := codec.NewBufferFromBytes(data)

	keyLen, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	valLen, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	keyBytes, err := buf.ReadBytes(int(keyLen))
	if err != nil {
		return nil, err
	}

	seqNum, err := buf.ReadUint64()
	if err != nil {
		return nil, err
	}

	t, err := buf.ReadUint8()
	if err != nil {
		return nil, err
	}

	valBytes, err := buf.ReadBytes(int(valLen))
	if err != nil {
		return nil, err
	}

	r := &Record{
		InternalKey: InternalKey{
			UserKey: string(keyBytes),
			SeqNum:  seqNum,
			Type:    IKType(t),
		},
		Value: valBytes,
	}

	return r, nil
}
