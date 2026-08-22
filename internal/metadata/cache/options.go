package cache

import (
	"fmt"
	"time"
)

// Config holds validated configuration for the metadata cache.
type Config struct {
	Enabled     bool
	MaxCost     int64         // Maximum memory cost (e.g., 256MB)
	NumCounters int64         // Ristretto recommendation: ~10x expected max items
	DefaultTTL  time.Duration // TTL for successful metadata hits
	NegativeTTL time.Duration // TTL for "not found" negative cache entries
	BufferItems int64         // Ristretto internal buffer size
}

// DefaultConfig returns a production-ready default configuration.
func DefaultConfig() Config {
	return Config{
		Enabled:     true,
		MaxCost:     256 * 1024 * 1024, // 256 MB
		NumCounters: 2560000,           // Safe default for millions of small items
		DefaultTTL:  5 * time.Minute,
		NegativeTTL: 30 * time.Second,
		BufferItems: 64,
	}
}

// Validate checks the configuration for correctness.
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}
	if c.MaxCost <= 0 {
		return fmt.Errorf("cache config: max_cost must be > 0")
	}
	if c.NumCounters <= 0 {
		return fmt.Errorf("cache config: num_counters must be > 0")
	}
	if c.DefaultTTL <= 0 {
		return fmt.Errorf("cache config: default_ttl must be > 0")
	}
	if c.NegativeTTL <= 0 {
		return fmt.Errorf("cache config: negative_ttl must be > 0")
	}
	return nil
}