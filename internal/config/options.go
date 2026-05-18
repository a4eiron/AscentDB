package config

import "time"

type Options struct {
	DataDir         string
	CrashRecovery   bool
	MemtableSize    uint32
	BlockSize       int
	WALSyncInterval time.Duration
}
