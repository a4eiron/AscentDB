package sstable

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math"
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

	return &Filter{
		bits: make([]byte, (m+7)/8),
		m:    m,
		k:    k,
	}
}

func (f *Filter) Add(key string) {
	h1 := hash1(key)
	h2 := hash2(key)

	for i := range uint64(f.k) {
		idx := (h1 + i*h2) % uint64(f.m)
		f.bits[idx/8] |= 1 << (idx % 8)
	}
}

func (f *Filter) Contains(key string) bool {
	h1 := hash1(key)
	h2 := hash2(key)
	for i := range uint64(f.k) {
		idx := (h1 + i*h2) % uint64(f.m)
		if f.bits[idx/8]&(1<<(idx%8)) == 0 {
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

// [bits_len(4)][bits][m(4)][k(1)]
func EncodeFilter(f *Filter) []byte {
	buf := make([]byte, 4+len(f.bits)+4+1)

	off := 0

	binary.LittleEndian.PutUint32(buf[off:], uint32(len(f.bits)))
	off += 4

	copy(buf[off:], f.bits)
	off += len(f.bits)

	binary.LittleEndian.PutUint32(buf[off:], f.m)
	off += 4

	buf[off] = f.k

	return buf
}

func DecodeFilter(b []byte) (*Filter, error) {
	if len(b) < 4 {
		return nil, errors.New("filter: buffer too small")
	}

	off := 0

	bitsLen := int(binary.LittleEndian.Uint32(b[off:]))
	off += 4

	if off+bitsLen+4+1 > len(b) {
		return nil, errors.New("filter: buffer too small")
	}

	bits := make([]byte, bitsLen)

	copy(bits, b[off:off+bitsLen])
	off += bitsLen

	m := binary.LittleEndian.Uint32(b[off:])
	off += 4

	k := b[off]
	return &Filter{bits: bits, m: m, k: k}, nil
}
