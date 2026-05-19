package sstable

import (
	"fmt"
	"testing"
)

func TestFilter_BasicOperations(t *testing.T) {
	f := NewFilter(1000, 0.01)

	keys := []string{"apple", "banana", "cherry", "dragonfruit", "elderberry"}

	for _, key := range keys {
		if f.Contains(key) {
			t.Errorf("filter should be empty, but found key:%v", key)
		}
	}

	for _, key := range keys {
		f.Add(key)
	}

	for _, key := range keys {
		if !f.Contains(key) {
			t.Errorf("filter mmissed an added key:%v", key)
		}
	}

}

func TestFilter_FalsePositives(t *testing.T) {
	n := 10000
	targetRate := 0.01

	f := NewFilter(n, targetRate)
	for i := range n {
		f.Add(fmt.Sprintf("key-%d", i))
	}

	queries := 1000
	fp := 0
	for i := range queries {
		if f.Contains(fmt.Sprintf("notkey-%d", i)) {
			fp++
		}
	}

	actualRate := float64(fp) / float64(queries)

	if actualRate > targetRate*1.5 {
		t.Fatalf("false positive rate too high: got %.2f%%, want < %.2f%%",
			actualRate*100, targetRate*200)
	}

	t.Logf("false positives: %d/%d (%.2f%%)", fp, queries, actualRate*100)
}

func TestFilter_EncodeDecode(t *testing.T) {
	f := NewFilter(1000, 0.01)

	keys := []string{
		"apple",
		"banana",
		"cherry",
		"dragonfruit",
		"elderberry",
	}

	for _, key := range keys {
		f.Add(key)
	}

	encoded := EncodeFilter(f)

	decoded, err := DecodeFilter(encoded)
	if err != nil {
		t.Fatalf("failed to decode filter: %v", err)
	}

	if decoded.m != f.m {
		t.Fatalf("m mismatch: got %d want %d", decoded.m, f.m)
	}

	if decoded.k != f.k {
		t.Fatalf("k mismatch: got %d want %d", decoded.k, f.k)
	}

	if len(decoded.bits) != len(f.bits) {
		t.Fatalf("bits length mismatch: got %d want %d",
			len(decoded.bits), len(f.bits))
	}

	for i := range f.bits {
		if decoded.bits[i] != f.bits[i] {
			t.Fatalf("bit mismatch at index %d: got %08b want %08b",
				i, decoded.bits[i], f.bits[i])
		}
	}

	for _, key := range keys {
		if !decoded.Contains(key) {
			t.Fatalf("decoded filter missing key: %s", key)
		}
	}

	nonKeys := []string{
		"kiwi",
		"mango",
		"papaya",
	}

	for _, key := range nonKeys {
		if decoded.Contains(key) {
			t.Logf("possible false positive for key: %s", key)
		}
	}
}

func TestFilter_DecodeCorrupted(t *testing.T) {
	f := NewFilter(1000, 0.01)
	f.Add("hello")

	encoded := EncodeFilter(f)

	corrupted := encoded[:len(encoded)-2]

	_, err := DecodeFilter(corrupted)
	if err == nil {
		t.Fatal("expected error for corrupted buffer")
	}
}

func Benchmark_Filter(b *testing.B) {
	b.Run("Add", func(b *testing.B) {
		f := NewFilter(10000, 0.01)

		keys := make([]string, b.N)
		for i := 0; i < b.N; i++ {
			keys[i] = fmt.Sprintf("bench-key-%d", i)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			f.Add(keys[i])
		}
	})

	b.Run("Contains", func(b *testing.B) {
		f := NewFilter(10000, 0.01)

		keys := make([]string, b.N)
		for i := 0; i < b.N; i++ {
			keys[i] = fmt.Sprintf("bench-key-%d", i)
			f.Add(keys[i])
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = f.Contains(keys[i])
		}
	})
}
