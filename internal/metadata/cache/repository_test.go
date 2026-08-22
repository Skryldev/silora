package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/Skryldev/silora/internal/metadata"
)

// mockRepo simulates the underlying Pebble repository.
type mockRepo struct {
	metadata.MetadataRepository
	mu       sync.Mutex
	getCalls int32
	objects  map[string]*metadata.ObjectMetadata
	err      error
}

func newMockRepo() *mockRepo {
	return &mockRepo{objects: make(map[string]*metadata.ObjectMetadata)}
}

func (m *mockRepo) Get(ctx context.Context, id string) (*metadata.ObjectMetadata, error) {
	atomic.AddInt32(&m.getCalls, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return nil, m.err
	}
	obj, ok := m.objects[id]
	if !ok {
		return nil, metadata.ErrMetadataNotFound
	}
	return obj, nil
}

func (m *mockRepo) Create(ctx context.Context, obj *metadata.ObjectMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[obj.ID] = obj
	return nil
}

func (m *mockRepo) Delete(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, id)
	return nil
}

func (m *mockRepo) HealthCheck(ctx context.Context) error { return nil }
func (m *mockRepo) Close() error                          { return nil }

func newTestCachedRepo(t *testing.T, next metadata.MetadataRepository) *CachedMetadataRepository {
	t.Helper()
	cfg := DefaultConfig()
	cfg.DefaultTTL = 1 * time.Second
	cfg.NegativeTTL = 500 * time.Millisecond
	
	logger := zap.NewNop()
	metrics := NewCacheMetrics(nil)
	
	repo, err := NewCachedMetadataRepository(next, cfg, logger, metrics)
	if err != nil {
		t.Fatalf("failed to create cached repo: %v", err)
	}
	return repo
}

func TestCache_HitAndMiss(t *testing.T) {
	mock := newMockRepo()
	repo := newTestCachedRepo(t, mock)
	ctx := context.Background()

	obj := &metadata.ObjectMetadata{ID: "id1", Bucket: "b", Key: "k"}
	mock.objects["id1"] = obj

	// First call: Miss
	m, err := repo.Get(ctx, "id1")
	if err != nil || m.ID != "id1" {
		t.Fatalf("expected hit, got err: %v", err)
	}
	if atomic.LoadInt32(&mock.getCalls) != 1 {
		t.Fatal("expected 1 underlying call")
	}

	// Second call: Hit
	m, err = repo.Get(ctx, "id1")
	if err != nil || m.ID != "id1" {
		t.Fatalf("expected hit, got err: %v", err)
	}
	if atomic.LoadInt32(&mock.getCalls) != 1 {
		t.Fatal("expected still 1 underlying call (cache hit)")
	}
}

func TestCache_Immutability(t *testing.T) {
	mock := newMockRepo()
	repo := newTestCachedRepo(t, mock)
	ctx := context.Background()

	obj := &metadata.ObjectMetadata{ID: "id1", Bucket: "b", Key: "k", Metadata: map[string]string{"a": "b"}}
	mock.objects["id1"] = obj

	m1, _ := repo.Get(ctx, "id1")
	m1.Metadata["a"] = "mutated" // Mutate the returned object

	m2, _ := repo.Get(ctx, "id1")
	if m2.Metadata["a"] == "mutated" {
		t.Fatal("cache state was mutated by caller! Defensive copy failed.")
	}
}

func TestCache_NegativeCaching(t *testing.T) {
	mock := newMockRepo()
	repo := newTestCachedRepo(t, mock)
	ctx := context.Background()

	_, err := repo.Get(ctx, "missing")
	if !errors.Is(err, metadata.ErrMetadataNotFound) {
		t.Fatal("expected not found")
	}
	if atomic.LoadInt32(&mock.getCalls) != 1 {
		t.Fatal("expected 1 call")
	}

	_, err = repo.Get(ctx, "missing")
	if !errors.Is(err, metadata.ErrMetadataNotFound) {
		t.Fatal("expected not found")
	}
	if atomic.LoadInt32(&mock.getCalls) != 1 {
		t.Fatal("expected still 1 call (negative cache hit)")
	}
}

func TestCache_StampedeProtection(t *testing.T) {
	mock := newMockRepo()
	repo := newTestCachedRepo(t, mock)
	ctx := context.Background()

	obj := &metadata.ObjectMetadata{ID: "hot", Bucket: "b", Key: "k"}
	mock.objects["hot"] = obj

	const workers = 100
	var wg sync.WaitGroup
	errCh := make(chan error, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := repo.Get(ctx, "hot")
			errCh <- err
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf("worker error: %v", err)
		}
	}

	calls := atomic.LoadInt32(&mock.getCalls)
	// Singleflight should bound this to a very small number (ideally 1, maybe 2 due to timing).
	if calls > 5 {
		t.Fatalf("stampede detected: underlying store was called %d times", calls)
	}
}

func TestCache_Invalidation(t *testing.T) {
	mock := newMockRepo()
	repo := newTestCachedRepo(t, mock)
	ctx := context.Background()

	obj := &metadata.ObjectMetadata{ID: "id1", Bucket: "b", Key: "k", Size: 100}
	mock.objects["id1"] = obj

	repo.Get(ctx, "id1") // Populate cache

	// Update
	obj.Size = 200
	mock.objects["id1"] = obj
	
	// The mock doesn't implement Update, so we simulate it by calling the cache's Update directly
	// which will call next.Update (which is a no-op in mock) and then invalidate.
	// To make this test work cleanly with the mock, we'll just call the cache invalidation logic.
	repo.invalidateAndSet(ctx, obj)

	m, _ := repo.Get(ctx, "id1")
	if m.Size != 200 {
		t.Fatal("cache returned stale data after invalidation")
	}
}