package cache

import (
	"context"
	"time"

	"github.com/dgraph-io/ristretto/v2"
)

// RistrettoCache implements MetadataCache using Dgraph's Ristretto v2 (Generics).
// K = string (cache key), V = *cacheValue (cached payload).
type RistrettoCache struct {
	cache   *ristretto.Cache[string, *cacheValue]
	metrics *CacheMetrics
}

func NewRistrettoCache(cfg Config, metrics *CacheMetrics) (*RistrettoCache, error) {
	c, err := ristretto.NewCache(&ristretto.Config[string, *cacheValue]{
		NumCounters: cfg.NumCounters,
		MaxCost:     cfg.MaxCost,
		BufferItems: cfg.BufferItems,
		Metrics:     true,
		Cost: func(value *cacheValue) int64 {
			return estimateCost(value)
		},
		OnEvict: func(item *ristretto.Item[*cacheValue]) {
			if metrics != nil {
				metrics.EvictionsTotal.Inc()
			}
		},
		OnReject: func(item *ristretto.Item[*cacheValue]) {
			if metrics != nil {
				metrics.RejectsTotal.Inc()
			}
		},
	})
	if err != nil {
		return nil, err
	}

	return &RistrettoCache{
		cache:   c,
		metrics: metrics,
	}, nil
}

func (r *RistrettoCache) Get(ctx context.Context, key string) (*cacheValue, bool) {
	// val is now strictly typed as *cacheValue. No type assertion needed!
	val, found := r.cache.Get(key)
	if !found {
		if r.metrics != nil {
			r.metrics.MissesTotal.Inc()
		}
		return nil, false
	}
	
	if val == nil {
		r.cache.Del(key) // Corrupt entry, delete it
		if r.metrics != nil {
			r.metrics.MissesTotal.Inc()
		}
		return nil, false
	}

	if r.metrics != nil {
		r.metrics.HitsTotal.Inc()
	}
	return val, true
}

func (r *RistrettoCache) Set(ctx context.Context, key string, val *cacheValue, ttl time.Duration) bool {
	if r.metrics != nil {
		r.metrics.SetsTotal.Inc()
	}
	// Cost is calculated automatically by the Cost function defined in Config.
	return r.cache.SetWithTTL(key, val, 0, ttl)
}

func (r *RistrettoCache) Delete(ctx context.Context, key string) {
	if r.metrics != nil {
		r.metrics.DeletesTotal.Inc()
	}
	r.cache.Del(key)
}

func (r *RistrettoCache) Clear(ctx context.Context) {
	r.cache.Clear()
}

func (r *RistrettoCache) Close() error {
	r.cache.Close()
	return nil
}

func (r *RistrettoCache) Wait() {
	r.cache.Wait()
}
// estimateCost calculates the approximate memory footprint of the cached value.
// Because of generics, 'val' is strictly typed as *cacheValue.
func estimateCost(val *cacheValue) int64 {
	cost := int64(64) // Base overhead + average key size
	if val == nil || val.neg {
		return cost + 16
	}
	m := val.meta
	if m == nil {
		return cost
	}
	
	cost += int64(len(m.ID) + len(m.Bucket) + len(m.Key) + len(m.ContentType) + len(m.ETag) + len(m.Checksum))
	cost += 128 // Struct fields & time.Time overhead
	
	for k, v := range m.Metadata {
		cost += int64(len(k) + len(v) + 48) // Map entry overhead
	}
	return cost
}