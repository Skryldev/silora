package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/Skryldev/silora/internal/metadata"
	"github.com/Skryldev/silora/internal/metadata/pebble"
)

func setupBenchRepo(b *testing.B, useCache bool) (metadata.MetadataRepository, func()) {
	b.Helper()
	dir := b.TempDir()
	cfg := pebble.DefaultConfig(dir)
	cfg.WALSync = false

	logger := zap.NewNop()
	pebbleRepo, err := pebble.NewPebbleMetadataRepository(cfg, logger)
	if err != nil {
		b.Fatalf("failed to create pebble repo: %v", err)
	}

	var repo metadata.MetadataRepository = pebbleRepo

	if useCache {
		cacheCfg := DefaultConfig()
		cacheCfg.DefaultTTL = 10 * time.Minute
		metrics := NewCacheMetrics(nil)
		cachedRepo, err := NewCachedMetadataRepository(pebbleRepo, cacheCfg, logger, metrics)
		if err != nil {
			b.Fatalf("failed to create cached repo: %v", err)
		}
		repo = cachedRepo
	}

	cleanup := func() {
		repo.Close()
	}

	// Seed data
	ctx := context.Background()
	for i := 0; i < 10000; i++ {
		id := uuid.New().String()
		m := &metadata.ObjectMetadata{
			ID:     id,
			Bucket: "bench",
			Key:    fmt.Sprintf("key-%d", i),
			Size:   1024,
		}
		if err := repo.Create(ctx, m); err != nil {
			b.Fatalf("seed failed: %v", err)
		}
	}

	return repo, cleanup
}

func BenchmarkGet_PebbleOnly(b *testing.B) {
	repo, cleanup := setupBenchRepo(b, false)
	defer cleanup()

	ctx := context.Background()	
	// Actually, let's just do a GetByKey which is deterministic
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := repo.GetByKey(ctx, "bench", fmt.Sprintf("key-%d", i%10000))
		if err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

func BenchmarkGet_Cached(b *testing.B) {
	repo, cleanup := setupBenchRepo(b, true)
	defer cleanup()

	ctx := context.Background()
	
	// Warm up cache
	for i := 0; i < 1000; i++ {
		repo.GetByKey(ctx, "bench", fmt.Sprintf("key-%d", i))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := repo.GetByKey(ctx, "bench", fmt.Sprintf("key-%d", i%1000)) // Hot keys
		if err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}