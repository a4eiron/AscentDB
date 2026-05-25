package record

import "github.com/a4eiron/ascentdb/internal/codec"

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
	payload := codec.NewBuffer(int(r.Size()))

	EncodeInternalKey(payload, r.InternalKey)
	payload.WriteUint32(r.ValueLen())
	payload.WriteBytes(r.Value)

	return payload.Bytes()
}

func DecodeRecord(data []byte) (Record, error) {
	buf := codec.NewBufferFromBytes(data)

	internalKey, err := DecodeInternalKey(buf)
	if err != nil {
		return Record{}, err
	}
	valLen, err := buf.ReadUint32()
	if err != nil {
		return Record{}, err
	}

	valBytes, err := buf.ReadBytes(int(valLen))
	if err != nil {
		return Record{}, err
	}

	r := Record{
		InternalKey: internalKey,
		Value:       valBytes,
	}

	return r, nil
}
