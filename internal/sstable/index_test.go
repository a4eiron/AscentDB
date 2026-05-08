package sstable

import (
	"testing"

	"github.com/a4eiron/ascentdb/internal/record"
)

func TestIndexEntryEncodeDecode(t *testing.T) {

	original := IndexEntry{
		SeparatorKey: record.InternalKey{
			UserKey: "key-42",
			SeqNum:  99,
			Type:    record.TypePut,
		},
		BlockOffset: 4096,
		BlockSize:   1024,
	}

	encoded := encodeIndexEntry(original)

	decoded, err := decodeIndexEntry(encoded)
	if err != nil {
		t.Fatalf("decodeIndexEntry failed: %v", err)
	}

	if decoded.BlockOffset != original.BlockOffset {
		t.Fatalf(
			"block offset mismatch: got=%d want=%d",
			decoded.BlockOffset,
			original.BlockOffset,
		)
	}

	if decoded.BlockSize != original.BlockSize {
		t.Fatalf(
			"block size mismatch: got=%d want=%d",
			decoded.BlockSize,
			original.BlockSize,
		)
	}

	if decoded.SeparatorKey.UserKey != original.SeparatorKey.UserKey {
		t.Fatalf(
			"user key mismatch: got=%s want=%s",
			decoded.SeparatorKey.UserKey,
			original.SeparatorKey.UserKey,
		)
	}

	if decoded.SeparatorKey.SeqNum != original.SeparatorKey.SeqNum {
		t.Fatalf(
			"seqnum mismatch: got=%d want=%d",
			decoded.SeparatorKey.SeqNum,
			original.SeparatorKey.SeqNum,
		)
	}

	if decoded.SeparatorKey.Type != original.SeparatorKey.Type {
		t.Fatalf(
			"type mismatch: got=%d want=%d",
			decoded.SeparatorKey.Type,
			original.SeparatorKey.Type,
		)
	}
}

func TestIndexBlockEncodeDecode(t *testing.T) {

	original := IndexBlock{
		entries: []IndexEntry{
			{
				SeparatorKey: record.InternalKey{
					UserKey: "key-1",
					SeqNum:  1,
					Type:    record.TypePut,
				},
				BlockOffset: 0,
				BlockSize:   4096,
			},
			{
				SeparatorKey: record.InternalKey{
					UserKey: "key-5",
					SeqNum:  7,
					Type:    record.TypeDel,
				},
				BlockOffset: 4096,
				BlockSize:   2048,
			},
			{
				SeparatorKey: record.InternalKey{
					UserKey: "key-9",
					SeqNum:  10,
					Type:    record.TypePut,
				},
				BlockOffset: 6144,
				BlockSize:   8192,
			},
		},
	}

	encoded := encodeIndexBlock(original)

	decoded, err := decodeIndexBlock(encoded)
	if err != nil {
		t.Fatalf("decodeIndexBlock failed: %v", err)
	}

	if len(decoded.entries) != len(original.entries) {
		t.Fatalf(
			"entry count mismatch: got=%d want=%d",
			len(decoded.entries),
			len(original.entries),
		)
	}

	for i := range original.entries {

		got := decoded.entries[i]
		want := original.entries[i]

		if got.BlockOffset != want.BlockOffset {
			t.Fatalf(
				"block offset mismatch at %d: got=%d want=%d",
				i,
				got.BlockOffset,
				want.BlockOffset,
			)
		}

		if got.BlockSize != want.BlockSize {
			t.Fatalf(
				"block size mismatch at %d: got=%d want=%d",
				i,
				got.BlockSize,
				want.BlockSize,
			)
		}

		if got.SeparatorKey.UserKey != want.SeparatorKey.UserKey {
			t.Fatalf(
				"user key mismatch at %d: got=%s want=%s",
				i,
				got.SeparatorKey.UserKey,
				want.SeparatorKey.UserKey,
			)
		}

		if got.SeparatorKey.SeqNum != want.SeparatorKey.SeqNum {
			t.Fatalf(
				"seqnum mismatch at %d: got=%d want=%d",
				i,
				got.SeparatorKey.SeqNum,
				want.SeparatorKey.SeqNum,
			)
		}

		if got.SeparatorKey.Type != want.SeparatorKey.Type {
			t.Fatalf(
				"type mismatch at %d: got=%d want=%d",
				i,
				got.SeparatorKey.Type,
				want.SeparatorKey.Type,
			)
		}
	}
}
