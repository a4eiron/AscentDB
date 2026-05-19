package sstable

import (
	"hash/fnv"
	"math"

	"github.com/a4eiron/ascentdb/internal/codec"
)

type Filter struct {
	bits []byte
	m    uint32
	k    uint8
}

func NewFilter(n int, p float64) *Filter {
	// no.of bits
	m := uint32(math.Ceil(-float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)))

	// no.of hash functions
	k := uint8(math.Round(float64(m) / float64(n) * math.Ln2))

	if k == 0 {
		k = 1
	}

	byteSize := (m + 7) / 8

	f := &Filter{
		bits: make([]byte, byteSize),
		m:    m,
		k:    k,
	}
	return f
}

func (f *Filter) Add(key string) {
	h1 := hash1(key)
	h2 := hash2(key)

	for i := range uint64(f.k) {
		idx := (h1 + i*h2) % uint64(f.m)

		byteIdx := idx / 8
		bitIdx := idx % 8

		f.bits[byteIdx] |= (1 << bitIdx)
	}

}

func (f *Filter) Contains(key string) bool {

	h1 := hash1(key)
	h2 := hash2(key)

	for i := range uint64(f.k) {
		idx := (h1 + i*h2) % uint64(f.m)

		byteIdx := idx / 8
		bitIdx := idx % 8

		if f.bits[byteIdx]&(1<<bitIdx) == 0 {
			return false
		}
	}
	return true
}

func hash1(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

func hash2(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	h.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF})
	return h.Sum64()
}

func EncodeFilter(f *Filter) []byte {
	totalSize := 4 + len(f.bits) + 4 + 1
	buf := codec.NewBuffer(totalSize)

	buf.WriteUint32(uint32(len(f.bits)))
	buf.WriteBytes(f.bits)
	buf.WriteUint32(f.m)
	buf.WriteUint8(f.k)

	return buf.Bytes()
}

func DecodeFilter(b []byte) (*Filter, error) {
	buf := codec.NewBufferFromBytes(b)
	bitsLen, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	bits, err := buf.ReadBytes(int(bitsLen))
	if err != nil {
		return nil, err
	}

	m, err := buf.ReadUint32()
	if err != nil {
		return nil, err
	}

	k, err := buf.ReadUint8()
	if err != nil {
		return nil, err
	}

	return &Filter{
		bits: bits,
		m:    m,
		k:    k,
	}, nil
}
