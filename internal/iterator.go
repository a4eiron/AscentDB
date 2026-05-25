package internal

import "github.com/a4eiron/ascentdb/internal/record"

type Iterator interface {
	Valid() bool
	Next()

	Key() record.InternalKey
	Value() []byte

	Seek(target record.InternalKey)
}
