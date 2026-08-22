package pebble

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/Skryldev/silora/internal/metadata"
)

func setupBenchRepo(b *testing.B) (*PebbleMetadataRepository, func()) {
	b.Helper()
	dir := b.TempDir()
	cfg := DefaultConfig(dir)
	cfg.WALSync = false // Maximize throughput for benchmarks

	logger := zap.NewNop()
	repo, err := NewPebbleMetadataRepository(cfg, logger)
	if err != nil {
		b.Fatalf("failed to create bench repo: %v", err)
	}

	cleanup := func() {
		repo.Close()
	}

	return repo, cleanup
}

func BenchmarkCreate(b *testing.B) {
	repo, cleanup := setupBenchRepo(b)
	defer cleanup()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		m := &metadata.ObjectMetadata{
			ID:        uuid.New().String(),
			Bucket:    "bench-bucket",
			Key:       fmt.Sprintf("key-%d", i),
			Size:      1024,
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		if err := repo.Create(ctx, m); err != nil {
			b.Fatalf("create failed: %v", err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	repo, cleanup := setupBenchRepo(b)
	defer cleanup()

	ctx := context.Background()

	// Seed data
	const seedCount = 10000
	ids := make([]string, seedCount)
	for i := 0; i < seedCount; i++ {
		id := uuid.New().String()
		ids[i] = id
		m := &metadata.ObjectMetadata{
			ID:     id,
			Bucket: "bench-bucket",
			Key:    fmt.Sprintf("key-%d", i),
		}
		if err := repo.Create(ctx, m); err != nil {
			b.Fatalf("seed failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		id := ids[i%seedCount]
		if _, err := repo.Get(ctx, id); err != nil {
			b.Fatalf("get failed: %v", err)
		}
	}
}

func BenchmarkGetByKey(b *testing.B) {
	repo, cleanup := setupBenchRepo(b)
	defer cleanup()

	ctx := context.Background()

	// Seed data
	const seedCount = 10000
	keys := make([]string, seedCount)
	for i := 0; i < seedCount; i++ {
		key := fmt.Sprintf("key-%d", i)
		keys[i] = key
		m := &metadata.ObjectMetadata{
			ID:     uuid.New().String(),
			Bucket: "bench-bucket",
			Key:    key,
		}
		if err := repo.Create(ctx, m); err != nil {
			b.Fatalf("seed failed: %v", err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := keys[i%seedCount]
		if _, err := repo.GetByKey(ctx, "bench-bucket", key); err != nil {
			b.Fatalf("get_by_key failed: %v", err)
		}
	}
}

func BenchmarkCreateMany(b *testing.B) {
	repo, cleanup := setupBenchRepo(b)
	defer cleanup()

	ctx := context.Background()
	const batchSize = 100

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		records := make([]*metadata.ObjectMetadata, batchSize)
		for j := 0; j < batchSize; j++ {
			records[j] = &metadata.ObjectMetadata{
				ID:     uuid.New().String(),
				Bucket: "bench-bucket",
				Key:    fmt.Sprintf("batch-%d-%d", i, j),
			}
		}
		if err := repo.CreateMany(ctx, records); err != nil {
			b.Fatalf("create_many failed: %v", err)
		}
	}
}