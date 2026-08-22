package cache

import (
	"context"
	"errors"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/Skryldev/silora/internal/metadata"
)

// CachedMetadataRepository decorates a MetadataRepository with a Ristretto cache.
type CachedMetadataRepository struct {
	next    metadata.MetadataRepository
	cache   MetadataCache
	sf      singleflight.Group
	cfg     Config
	logger  *zap.Logger
	metrics *CacheMetrics
}

var _ metadata.MetadataRepository = (*CachedMetadataRepository)(nil)

func NewCachedMetadataRepository(
	next metadata.MetadataRepository,
	cfg Config,
	logger *zap.Logger,
	metrics *CacheMetrics,
) (*CachedMetadataRepository, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	var c MetadataCache
	if cfg.Enabled {
		rc, err := NewRistrettoCache(cfg, metrics)
		if err != nil {
			return nil, err
		}
		c = rc
		logger.Info("metadata cache enabled", zap.Int64("max_cost", cfg.MaxCost))
	} else {
		c = NoopCache{}
		logger.Info("metadata cache disabled")
	}

	return &CachedMetadataRepository{
		next: next, cache: c, cfg: cfg, logger: logger, metrics: metrics,
	}, nil
}

// cloneMetadata deep-copies metadata to prevent callers from mutating cached state.
func cloneMetadata(m *metadata.ObjectMetadata) *metadata.ObjectMetadata {
	if m == nil {
		return nil
	}
	cp := *m
	if m.Metadata != nil {
		cp.Metadata = make(map[string]string, len(m.Metadata))
		for k, v := range m.Metadata {
			cp.Metadata[k] = v
		}
	}
	return &cp
}

func (r *CachedMetadataRepository) Create(ctx context.Context, m *metadata.ObjectMetadata) error {
	err := r.next.Create(ctx, m)
	if err != nil {
		return err
	}
	r.invalidateAndSet(ctx, m)
	return nil
}

func (r *CachedMetadataRepository) Get(ctx context.Context, id string) (*metadata.ObjectMetadata, error) {
	key := KeyByID(id)
	return r.getWithSingleflight(ctx, key, func(ctx context.Context) (*metadata.ObjectMetadata, error) {
		return r.next.Get(ctx, id)
	})
}

func (r *CachedMetadataRepository) GetByKey(ctx context.Context, bucket, key string) (*metadata.ObjectMetadata, error) {
	cacheKey := KeyByBucketAndKey(bucket, key)
	return r.getWithSingleflight(ctx, cacheKey, func(ctx context.Context) (*metadata.ObjectMetadata, error) {
		m, err := r.next.GetByKey(ctx, bucket, key)
		if err == nil && m != nil {
			// Warm the ID cache as well
			idKey := KeyByID(m.ID)
			val := &cacheValue{meta: cloneMetadata(m), neg: false}
			r.cache.Set(ctx, idKey, val, r.cfg.DefaultTTL)
		}
		return m, err
	})
}

func (r *CachedMetadataRepository) getWithSingleflight(
	ctx context.Context, cacheKey string,
	loadFn func(ctx context.Context) (*metadata.ObjectMetadata, error),
) (*metadata.ObjectMetadata, error) {
	val, found := r.cache.Get(ctx, cacheKey)
	if found {
		if val.neg {
			return nil, metadata.ErrMetadataNotFound
		}
		return cloneMetadata(val.meta), nil
	}

	v, err, _ := r.sf.Do(cacheKey, func() (interface{}, error) {
		// Double-check cache inside singleflight
		val, found := r.cache.Get(ctx, cacheKey)
		if found {
			return val, nil
		}

		start := time.Now()
		m, loadErr := loadFn(ctx)
		duration := time.Since(start)
		if r.metrics != nil {
			r.metrics.LoadDuration.Observe(duration.Seconds())
		}

		if loadErr != nil {
			if errors.Is(loadErr, metadata.ErrMetadataNotFound) {
				negVal := &cacheValue{neg: true}
				r.cache.Set(ctx, cacheKey, negVal, r.cfg.NegativeTTL)
				return negVal, nil // Return negative sentinel, no error
			}
			if r.metrics != nil {
				r.metrics.ErrorsTotal.Inc()
			}
			return nil, loadErr
		}

		cv := &cacheValue{meta: cloneMetadata(m), neg: false}
		r.cache.Set(ctx, cacheKey, cv, r.cfg.DefaultTTL)
		return cv, nil
	})

	if err != nil {
		return nil, err
	}

	cv, ok := v.(*cacheValue)
	if !ok {
		return nil, errors.New("cache: unexpected singleflight return type")
	}
	if cv.neg {
		return nil, metadata.ErrMetadataNotFound
	}

	return cloneMetadata(cv.meta), nil
}

func (r *CachedMetadataRepository) Update(ctx context.Context, m *metadata.ObjectMetadata) error {
	err := r.next.Update(ctx, m)
	if err != nil {
		return err
	}
	r.invalidateAndSet(ctx, m)
	return nil
}

func (r *CachedMetadataRepository) Delete(ctx context.Context, id string) error {
	// Fetch to find bucket/key for secondary invalidation.
	// This ensures strict consistency for GetByKey lookups.
	m, _ := r.next.Get(ctx, id)

	err := r.next.Delete(ctx, id)
	if err != nil {
		return err
	}

	r.cache.Delete(ctx, KeyByID(id))
	negVal := &cacheValue{neg: true}
	r.cache.Set(ctx, KeyByID(id), negVal, r.cfg.NegativeTTL)

	if m != nil {
		r.cache.Delete(ctx, KeyByBucketAndKey(m.Bucket, m.Key))
		r.cache.Set(ctx, KeyByBucketAndKey(m.Bucket, m.Key), negVal, r.cfg.NegativeTTL)
	}
	return nil
}

func (r *CachedMetadataRepository) Exists(ctx context.Context, id string) (bool, error) {
	m, err := r.Get(ctx, id)
	if err != nil {
		if errors.Is(err, metadata.ErrMetadataNotFound) {
			return false, nil
		}
		return false, err
	}
	return m != nil, nil
}

func (r *CachedMetadataRepository) List(ctx context.Context, req *metadata.ListMetadataRequest) (metadata.MetadataIterator, error) {
	// Iterators bypass the cache to avoid memory explosion and stale pagination issues.
	return r.next.List(ctx, req)
}

func (r *CachedMetadataRepository) CreateMany(ctx context.Context, records []*metadata.ObjectMetadata) error {
	err := r.next.CreateMany(ctx, records)
	if err != nil {
		return err
	}
	for _, m := range records {
		r.invalidateAndSet(ctx, m)
	}
	return nil
}

func (r *CachedMetadataRepository) DeleteMany(ctx context.Context, ids []string) error {
	var metas []*metadata.ObjectMetadata
	for _, id := range ids {
		m, err := r.next.Get(ctx, id)
		if err == nil && m != nil {
			metas = append(metas, m)
		}
	}

	err := r.next.DeleteMany(ctx, ids)
	if err != nil {
		return err
	}

	negVal := &cacheValue{neg: true}
	for _, id := range ids {
		r.cache.Delete(ctx, KeyByID(id))
		r.cache.Set(ctx, KeyByID(id), negVal, r.cfg.NegativeTTL)
	}
	for _, m := range metas {
		r.cache.Delete(ctx, KeyByBucketAndKey(m.Bucket, m.Key))
		r.cache.Set(ctx, KeyByBucketAndKey(m.Bucket, m.Key), negVal, r.cfg.NegativeTTL)
	}
	return nil
}

func (r *CachedMetadataRepository) Close() error {
	err1 := r.cache.Close()
	err2 := r.next.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

func (r *CachedMetadataRepository) HealthCheck(ctx context.Context) error {
	if r.cfg.Enabled {
		testKey := "__health_check__"
		val := &cacheValue{neg: true}
		r.cache.Set(ctx, testKey, val, time.Second)
		_, _ = r.cache.Get(ctx, testKey)
		r.cache.Delete(ctx, testKey)
	}
	return r.next.HealthCheck(ctx)
}

func (r *CachedMetadataRepository) invalidateAndSet(ctx context.Context, m *metadata.ObjectMetadata) {
	r.cache.Delete(ctx, KeyByID(m.ID))
	r.cache.Delete(ctx, KeyByBucketAndKey(m.Bucket, m.Key))
	
	val := &cacheValue{meta: cloneMetadata(m), neg: false}
	r.cache.Set(ctx, KeyByID(m.ID), val, r.cfg.DefaultTTL)
	r.cache.Set(ctx, KeyByBucketAndKey(m.Bucket, m.Key), val, r.cfg.DefaultTTL)
}