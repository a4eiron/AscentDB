package config

type Options struct {
	DataDir       string
	CrashRecovery bool
	MemtableSize  uint32
	BlockSize     int
}
