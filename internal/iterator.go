package internal

import "github.com/a4eiron/ascentdb/internal/record"

type Iterator interface {
	Valid() bool
	Next()

	InternalKey() record.InternalKey
	Key() []byte
	Value() []byte

	Seek(target record.InternalKey)
}
