package config

import "time"

type SyncMode int

const (
	// intervaled sync, may lose last interval on crash
	SyncNone SyncMode = iota

	// group commit, blocks until fsync
	SyncBatch
)

type SyncOptions struct {
	Mode     SyncMode
	Interval time.Duration
}

type Options struct {
	DataDir       string
	CrashRecovery bool
	MemtableSize  uint32
	BlockSize     uint

	FilterExpectedKeys uint
	FilterFPRate       float64

	BlockCacheSize int

	MaxL0Files      uint
	MaxBytesBase    int64
	LevelMultiplier uint
	MaxSSTableSize  int64
	NumLevels       uint

	SkiplistMaxLevel uint
	SkiplistP        float64

	SyncOptions
}

func WithDefaults(opts *Options) *Options {
	if opts.FilterExpectedKeys == 0 {
		opts.FilterExpectedKeys = 1000
	}
	if opts.FilterFPRate == 0 {
		opts.FilterFPRate = 0.01
	}
	if opts.BlockCacheSize == 0 {
		opts.BlockCacheSize = 1024
	}
	if opts.MaxL0Files == 0 {
		opts.MaxL0Files = 4
	}
	if opts.MaxBytesBase == 0 {
		opts.MaxBytesBase = 10 * 1024 * 1024
	}
	if opts.LevelMultiplier == 0 {
		opts.LevelMultiplier = 10
	}
	if opts.MaxSSTableSize == 0 {
		opts.MaxSSTableSize = 4 * 1024 * 1024
	}
	if opts.NumLevels == 0 {
		opts.NumLevels = 7
	}
	if opts.SkiplistMaxLevel == 0 {
		opts.SkiplistMaxLevel = 16
	}
	if opts.SkiplistP == 0 {
		opts.SkiplistP = 0.25
	}
	return opts
}
