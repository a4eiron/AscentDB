package config

import "time"

type Options struct {
	DataDir       string
	CrashRecovery bool
	MemtableSize  uint32
	BlockSize     int
	SyncOptions
}

type SyncMode int

const (
	// interval sync, may lose last interval on crash
	SyncNone SyncMode = iota

	// group commit, blocks until fsync
	SyncBatch
)

type SyncOptions struct {
	Mode     SyncMode
	Interval time.Duration
}
