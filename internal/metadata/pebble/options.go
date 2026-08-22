package pebble

import (
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
)

// Config holds validated configuration for the Pebble metadata store.
type Config struct {
	// Path is the directory where Pebble stores its data.
	Path string

	// CacheSize is the size of the Pebble block cache in bytes.
	// Default: 64 MB.
	CacheSize int64

	// MaxOpenFiles limits the number of open file descriptors.
	// Default: 1024.
	MaxOpenFiles int

	// WALSync controls whether WAL writes are synced to disk.
	// true = fsync on every write (stronger durability, lower throughput).
	// false = OS-level buffering (higher throughput, risk of losing recent writes on crash).
	// Default: true.
	WALSync bool

	// WALDir is an optional separate directory for the WAL.
	// If empty, WAL is stored in the same directory as data.
	WALDir string

	// MemTableSize is the size of each memtable.
	// Default: 64 MB.
	MemTableSize int

	// MaxConcurrentCompactions limits parallel compaction goroutines.
	// Default: 2.
	MaxConcurrentCompactions int

	// L0CompactionThreshold is the number of L0 files that trigger compaction.
	// Default: 4.
	L0CompactionThreshold int

	// L0StopWritesThreshold is the number of L0 files that stall writes.
	// Default: 12.
	L0StopWritesThreshold int

	// DisableWAL disables the WAL entirely. Only for testing.
	// Default: false.
	DisableWAL bool

	// ReadOnly opens the database in read-only mode.
	// Default: false.
	ReadOnly bool
}

// DefaultConfig returns a production-ready default configuration.
func DefaultConfig(path string) Config {
	return Config{
		Path:                     path,
		CacheSize:                64 * 1024 * 1024, // 64 MB
		MaxOpenFiles:             1024,
		WALSync:                  true,
		MemTableSize:             64 * 1024 * 1024, // 64 MB
		MaxConcurrentCompactions: 2,
		L0CompactionThreshold:    4,
		L0StopWritesThreshold:    12,
	}
}

// Validate checks the configuration for correctness.
func (c *Config) Validate() error {
	if c.Path == "" {
		return fmt.Errorf("pebble config: path is required")
	}
	if c.CacheSize < 1024*1024 {
		return fmt.Errorf("pebble config: cache size must be at least 1MB, got %d", c.CacheSize)
	}
	if c.MaxOpenFiles < 10 {
		return fmt.Errorf("pebble config: max open files must be at least 10, got %d", c.MaxOpenFiles)
	}
	if c.MemTableSize < 1024*1024 {
		return fmt.Errorf("pebble config: memtable size must be at least 1MB, got %d", c.MemTableSize)
	}
	if c.MaxConcurrentCompactions < 1 {
		return fmt.Errorf("pebble config: max concurrent compactions must be at least 1")
	}
	return nil
}

// toPebbleOptions converts the Config to pebble.Options.
func (c *Config) toPebbleOptions() *pebble.Options {
	opts := &pebble.Options{
		Cache:                    pebble.NewCache(c.CacheSize),
		MaxOpenFiles:             c.MaxOpenFiles,
		MemTableSize:             uint64(c.MemTableSize),
		MaxConcurrentCompactions: func() int { return c.MaxConcurrentCompactions },
		L0CompactionThreshold:    c.L0CompactionThreshold,
		L0StopWritesThreshold:    c.L0StopWritesThreshold,
		ReadOnly:                 c.ReadOnly,
		DisableWAL:               c.DisableWAL,
	}

	if c.WALDir != "" {
		opts.WALDir = c.WALDir
	}

	if c.WALSync {
		opts.WALSyncDelay = 0
	} else {
		opts.WALSyncDelay = 100 * time.Millisecond
	}

	return opts
}