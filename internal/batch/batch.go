package batch

import "github.com/a4eiron/ascentdb/internal/record"

type Batch struct {
	recs []record.Record
}

func New() *Batch {
	return &Batch{}
}

func (b *Batch) Put(key, value []byte) *Batch {
	b.recs = append(b.recs, record.Record{
		InternalKey: record.InternalKey{
			UserKey: key,
			Type:    record.TypePut,
		},
		Value: value,
	})
	return b
}

func (b *Batch) Delete(key []byte) *Batch {
	b.recs = append(b.recs, record.Record{
		InternalKey: record.InternalKey{
			UserKey: key,
			Type:    record.TypeDel,
		},
		Value: nil,
	})
	return b
}

func (b *Batch) Len() int {
	return len(b.recs)
}

func (b *Batch) Reset() {
	b.recs = b.recs[:0]
}

func (b *Batch) Records() []record.Record {
	return b.recs
}

func (b *Batch) Size() int {
	total := 0
	for _, rec := range b.recs {
		total += 4 + int(rec.Size()) + 4
	}
	return total
}
