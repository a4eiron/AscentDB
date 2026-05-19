package sstable

import (
	"bytes"
	"testing"

	"github.com/a4eiron/ascentdb/internal/record"
)

func TestBlockEncodeDecode(t *testing.T) {

	original := &Block{
		entries: []record.Record{
			{
				InternalKey: record.InternalKey{
					UserKey: "key-1",
					SeqNum:  1,
					Type:    record.TypePut,
				},
				Value: []byte("val-1"),
			},
			{
				InternalKey: record.InternalKey{
					UserKey: "key-1",
					SeqNum:  2,
					Type:    record.TypeDel,
				},
				Value: nil,
			},
			{
				InternalKey: record.InternalKey{
					UserKey: "key-2",
					SeqNum:  3,
					Type:    record.TypePut,
				},
				Value: []byte("val-2"),
			},
		},
	}

	encoded := encodeBlock(original)

	decoded, err := decodeBlock(encoded)
	if err != nil {
		t.Fatalf("decodeBlock failed: %v", err)
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

		if got.UserKey != want.UserKey {
			t.Fatalf(
				"user key mismatch at %d: got=%s want=%s",
				i,
				got.UserKey,
				want.UserKey,
			)
		}

		if got.SeqNum != want.SeqNum {
			t.Fatalf(
				"seqnum mismatch at %d: got=%d want=%d",
				i,
				got.SeqNum,
				want.SeqNum,
			)
		}

		if got.Type != want.Type {
			t.Fatalf(
				"type mismatch at %d: got=%d want=%d",
				i,
				got.Type,
				want.Type,
			)
		}

		if !bytes.Equal(got.Value, want.Value) {
			t.Fatalf(
				"value mismatch at %d: got=%q want=%q",
				i,
				got.Value,
				want.Value,
			)
		}
	}
}
