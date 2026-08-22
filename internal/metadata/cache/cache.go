package cache

import (
	"context"
	"time"

	"github.com/Skryldev/silora/internal/metadata"
)

// cacheValue is the internal wrapper stored in Ristretto.
type cacheValue struct {
	meta *metadata.ObjectMetadata
	neg  bool // true if this is a "not found" sentinel
}

// MetadataCache defines the abstraction for the caching layer.
type MetadataCache interface {
	Get(ctx context.Context, key string) (*cacheValue, bool)
	Set(ctx context.Context, key string, val *cacheValue, ttl time.Duration) bool
	Delete(ctx context.Context, key string)
	Clear(ctx context.Context)
	Close() error
	Wait() // Blocks until all pending Set operations are processed by the admission policy
}

// NoopCache is used when the cache is disabled.
type NoopCache struct{}

func (NoopCache) Get(ctx context.Context, key string) (*cacheValue, bool) { return nil, false }
func (NoopCache) Set(ctx context.Context, key string, val *cacheValue, ttl time.Duration) bool { return false }
func (NoopCache) Delete(ctx context.Context, key string) {}
func (NoopCache) Clear(ctx context.Context)              {}
func (NoopCache) Close() error                           { return nil }
func (NoopCache) Wait()                                  {}