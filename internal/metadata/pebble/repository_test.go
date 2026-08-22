package pebble

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/Skryldev/silora/internal/metadata"
)

func newTestRepo(t *testing.T) (*PebbleMetadataRepository, func()) {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.WALSync = false // Faster for tests
	cfg.CacheSize = 8 * 1024 * 1024 // 8MB

	logger := zap.NewNop()
	repo, err := NewPebbleMetadataRepository(cfg, logger)
	if err != nil {
		t.Fatalf("failed to create test repo: %v", err)
	}

	cleanup := func() {
		if err := repo.Close(); err != nil {
			t.Errorf("failed to close repo: %v", err)
		}
	}

	return repo, cleanup
}

func TestCreateAndGet(t *testing.T) {
	repo, cleanup := newTestRepo(t)
	defer cleanup()

	ctx := context.Background()
	now := time.Now().UTC()

	m := &metadata.ObjectMetadata{
		ID:          uuid.New().String(),
		Bucket:      "test-bucket",
		Key:         "file.txt",
		Size:        1024,
		ContentType: "text/plain",
		ETag:        "etag123",
		Version:     1,
		State:       metadata.StateAvailable,
		CreatedAt:   now,
		UpdatedAt:   now,
		Metadata:    map[string]string{"foo": "bar"},
	}

	err := repo.Create(ctx, m)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	// Get by ID
	got, err := repo.Get(ctx, m.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.ID != m.ID || got.Size != m.Size || got.Metadata["foo"] != "bar" {
		t.Fatalf("Get mismatch: got %+v", got)
	}

	// Get by Key
	gotByKey, err := repo.GetByKey(ctx, m.Bucket, m.Key)
	if err != nil {
		t.Fatalf("GetByKey failed: %v", err)
	}
	if gotByKey.ID != m.ID {
		t.Fatalf("GetByKey mismatch: got ID %q, want %q", gotByKey.ID, m.ID)
	}

	// Exists
	exists, err := repo.Exists(ctx, m.ID)
	if err != nil {
		t.Fatalf("Exists failed: %v", err)
	}
	if !exists {
		t.Fatal("Exists returned false")
	}
}

func TestCreate_Conflict(t *testing.T) {
	repo, cleanup := newTestRepo(t)
	defer cleanup()

	ctx := context.Background()
	m := &metadata.ObjectMetadata{
		ID:     uuid.New().String(),
		Bucket: "b",
		Key:    "k",
	}

	if err := repo.Create(ctx, m); err != nil {
		t.Fatalf("first create failed: %v", err)
	}

	err := repo.Create(ctx, m)
	if !errors.Is(err, metadata.ErrMetadataAlreadyExists) {
		t.Fatalf("expected ErrMetadataAlreadyExists, got %v", err)
	}
}

func TestUpdate_VersionConflict(t *testing.T) {
	repo, cleanup := newTestRepo(t)
	defer cleanup()

	ctx := context.Background()
	m := &metadata.ObjectMetadata{
		ID:      uuid.New().String(),
		Bucket:  "b",
		Key:     "k",
		Version: 1,
	}

	if err := repo.Create(ctx, m); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Update with correct version
	m.Size = 2048
	if err := repo.Update(ctx, m); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	// Try to update again with the OLD version (should fail)
	m.Version = 1
	m.Size = 4096
	err := repo.Update(ctx, m)
	if !errors.Is(err, metadata.ErrMetadataConflict) {
		t.Fatalf("expected ErrMetadataConflict, got %v", err)
	}
}

func TestDelete_Idempotent(t *testing.T) {
	repo, cleanup := newTestRepo(t)
	defer cleanup()

	ctx := context.Background()
	id := uuid.New().String()

	// Delete non-existent
	if err := repo.Delete(ctx, id); err != nil {
		t.Fatalf("delete non-existent failed: %v", err)
	}

	m := &metadata.ObjectMetadata{ID: id, Bucket: "b", Key: "k"}
	if err := repo.Create(ctx, m); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// Delete existing
	if err := repo.Delete(ctx, id); err != nil {
		t.Fatalf("delete existing failed: %v", err)
	}

	// Verify gone
	_, err := repo.Get(ctx, id)
	if !errors.Is(err, metadata.ErrMetadataNotFound) {
		t.Fatalf("expected ErrMetadataNotFound, got %v", err)
	}

	// Delete again (idempotent)
	if err := repo.Delete(ctx, id); err != nil {
		t.Fatalf("second delete failed: %v", err)
	}
}

func TestList_PaginationAndPrefix(t *testing.T) {
	repo, cleanup := newTestRepo(t)
	defer cleanup()

	ctx := context.Background()

	// Create 15 objects in bucket-a, 5 in bucket-b
	for i := 0; i < 15; i++ {
		m := &metadata.ObjectMetadata{
			ID:     uuid.New().String(),
			Bucket: "bucket-a",
			Key:    "docs/" + uuid.New().String(),
		}
		if err := repo.Create(ctx, m); err != nil {
			t.Fatalf("create failed: %v", err)
		}
	}
	for i := 0; i < 5; i++ {
		m := &metadata.ObjectMetadata{
			ID:     uuid.New().String(),
			Bucket: "bucket-b",
			Key:    "images/" + uuid.New().String(),
		}
		if err := repo.Create(ctx, m); err != nil {
			t.Fatalf("create failed: %v", err)
		}
	}

	// List all in bucket-a
	iter, err := repo.List(ctx, &metadata.ListMetadataRequest{
		Bucket: "bucket-a",
		Limit:  10,
	})
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	defer iter.Close()

	count := 0
	var cursor string
	for iter.Next() {
		item, err := iter.Item()
		if err != nil {
			t.Fatalf("item error: %v", err)
		}
		if item.Bucket != "bucket-a" {
			t.Fatalf("wrong bucket: %s", item.Bucket)
		}
		count++
		cursor = iter.Cursor()
	}
	if err := iter.Err(); err != nil {
		t.Fatalf("iterator error: %v", err)
	}
	iter.Close()

	if count != 10 {
		t.Fatalf("expected 10 items, got %d", count)
	}
	if cursor == "" {
		t.Fatal("expected non-empty cursor")
	}

	// Fetch page 2
	iter2, err := repo.List(ctx, &metadata.ListMetadataRequest{
		Bucket: "bucket-a",
		Limit:  10,
		Cursor: cursor,
	})
	if err != nil {
		t.Fatalf("list page 2 failed: %v", err)
	}
	defer iter2.Close() 

	count2 := 0
	for iter2.Next() {
		count2++
	}
	iter2.Close()

	if count2 != 5 {
		t.Fatalf("expected 5 items on page 2, got %d", count2)
	}
}

func TestBatchOperations(t *testing.T) {
	repo, cleanup := newTestRepo(t)
	defer cleanup()

	ctx := context.Background()

	records := make([]*metadata.ObjectMetadata, 50)
	ids := make([]string, 50)
	for i := 0; i < 50; i++ {
		id := uuid.New().String()
		records[i] = &metadata.ObjectMetadata{
			ID:     id,
			Bucket: "batch-bucket",
			Key:    "file-" + id,
		}
		ids[i] = id
	}

	// CreateMany
	if err := repo.CreateMany(ctx, records); err != nil {
		t.Fatalf("CreateMany failed: %v", err)
	}

	// Verify all exist
	for _, id := range ids {
		exists, err := repo.Exists(ctx, id)
		if err != nil || !exists {
			t.Fatalf("Exists failed for %s: %v", id, err)
		}
	}

	// DeleteMany
	if err := repo.DeleteMany(ctx, ids); err != nil {
		t.Fatalf("DeleteMany failed: %v", err)
	}

	// Verify all gone
	for _, id := range ids {
		exists, err := repo.Exists(ctx, id)
		if err != nil {
			t.Fatalf("Exists failed after delete: %v", err)
		}
		if exists {
			t.Fatalf("record %s still exists after DeleteMany", id)
		}
	}
}

func TestConcurrency(t *testing.T) {
	repo, cleanup := newTestRepo(t)
	defer cleanup()

	ctx := context.Background()
	const workers = 20
	const opsPerWorker = 50

	var wg sync.WaitGroup
	errCh := make(chan error, workers*opsPerWorker)

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < opsPerWorker; i++ {
				id := uuid.New().String()
				m := &metadata.ObjectMetadata{
					ID:     id,
					Bucket: "concurrent-bucket",
					Key:    "key-" + id,
				}
				if err := repo.Create(ctx, m); err != nil {
					errCh <- err
					return
				}
				if _, err := repo.Get(ctx, id); err != nil {
					errCh <- err
					return
				}
				if err := repo.Delete(ctx, id); err != nil {
					errCh <- err
					return
				}
			}
		}(w)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Errorf("concurrent error: %v", err)
	}
}

func TestRecovery(t *testing.T) {
	dir := t.TempDir()
	cfg := DefaultConfig(dir)
	cfg.WALSync = true // Ensure durability for recovery test
	logger := zap.NewNop()

	id := uuid.New().String()

	// Phase 1: Open, write, close
	repo1, err := NewPebbleMetadataRepository(cfg, logger)
	if err != nil {
		t.Fatalf("open 1 failed: %v", err)
	}

	m := &metadata.ObjectMetadata{
		ID:     id,
		Bucket: "recovery-bucket",
		Key:    "recovery-key",
		Size:   999,
	}
	if err := repo1.Create(context.Background(), m); err != nil {
		t.Fatalf("create failed: %v", err)
	}
	if err := repo1.Close(); err != nil {
		t.Fatalf("close 1 failed: %v", err)
	}

	// Phase 2: Reopen and verify
	repo2, err := NewPebbleMetadataRepository(cfg, logger)
	if err != nil {
		t.Fatalf("open 2 failed: %v", err)
	}
	defer repo2.Close()

	got, err := repo2.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get after recovery failed: %v", err)
	}
	if got.Size != 999 {
		t.Fatalf("data mismatch after recovery: got %d, want 999", got.Size)
	}
}