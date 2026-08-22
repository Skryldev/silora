package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"

	"github.com/Skryldev/silora/internal/metadata"
	"github.com/Skryldev/silora/internal/metadata/cache"
	pebblemeta "github.com/Skryldev/silora/internal/metadata/pebble"
	"github.com/Skryldev/silora/internal/storage"
	"github.com/Skryldev/silora/internal/storage/miniostorage"
)

func main() {
	// ---------------------------------------------------------------
	// Logger
	// ---------------------------------------------------------------
	logger, err := zap.NewProduction()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()
	sugar := logger.Sugar()

	// ---------------------------------------------------------------
	// Context + Graceful Shutdown
	// ---------------------------------------------------------------
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		sugar.Infow("shutdown signal received", "signal", sig.String())
		cancel()
	}()

	// ---------------------------------------------------------------
	// Phase 1: MinIO Storage Core
	// ---------------------------------------------------------------
	objectStore, err := initMinIO(logger)
	if err != nil {
		sugar.Fatalw("failed to initialize minio storage", "error", err)
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if err := objectStore.Shutdown(shutdownCtx); err != nil {
			sugar.Errorw("minio shutdown error", "error", err)
		}
	}()

	// ---------------------------------------------------------------
	// Phase 2: Pebble Metadata Repository
	// ---------------------------------------------------------------
	pebbleRepo, err := initPebble(logger)
	if err != nil {
		sugar.Fatalw("failed to initialize pebble metadata store", "error", err)
	}

	// ---------------------------------------------------------------
	// Phase 3: Ristretto Cache (Decorator around Pebble)
	// ---------------------------------------------------------------
	cachedRepo, err := initCache(pebbleRepo, logger)
	if err != nil {
		sugar.Fatalw("failed to initialize cache layer", "error", err)
	}
	defer func() {
		if err := cachedRepo.Close(); err != nil {
			sugar.Errorw("cache repo close error", "error", err)
		}
	}()

	// ---------------------------------------------------------------
	// Demonstration Workflow
	// ---------------------------------------------------------------
	if err := runDemo(ctx, sugar, objectStore, cachedRepo); err != nil {
		sugar.Fatalw("demo failed", "error", err)
	}

	sugar.Info("all phases integrated successfully. shutdown complete.")
}

// ---------------------------------------------------------------
// Initialization Helpers
// ---------------------------------------------------------------

func initMinIO(logger *zap.Logger) (*miniostorage.Storage, error) {
	cfg := miniostorage.ConfigFromEnv()

	if cfg.Endpoint == "" {
		cfg.Endpoint = "localhost:9000"
	}
	if cfg.AccessKey == "" {
		cfg.AccessKey = "admin"
	}
	if cfg.SecretKey == "" {
		cfg.SecretKey = "Mypassword1234"
	}
	cfg.Secure = false

	return miniostorage.New(cfg)
}

func initPebble(logger *zap.Logger) (*pebblemeta.PebbleMetadataRepository, error) {
	dbPath := os.Getenv("SILORA_PEBBLE_PATH")
	if dbPath == "" {
		dbPath = "./data/metadata"
	}

	cfg := pebblemeta.DefaultConfig(dbPath)
	cfg.WALSync = true

	return pebblemeta.NewPebbleMetadataRepository(cfg, logger)
}

func initCache(
	pebbleRepo metadata.MetadataRepository,
	logger *zap.Logger,
) (*cache.CachedMetadataRepository, error) {
	cacheCfg := cache.DefaultConfig()
	cacheCfg.Enabled = true
	cacheCfg.MaxCost = 128 * 1024 * 1024 // 128 MB
	cacheCfg.DefaultTTL = 5 * time.Minute
	cacheCfg.NegativeTTL = 30 * time.Second

	reg := prometheus.NewRegistry()
	metrics := cache.NewCacheMetrics(reg)

	return cache.NewCachedMetadataRepository(pebbleRepo, cacheCfg, logger, metrics)
}

// ---------------------------------------------------------------
// Demo Workflow
// ---------------------------------------------------------------

func runDemo(
	ctx context.Context,
	sugar *zap.SugaredLogger,
	objectStore storage.StorageCore,
	repo metadata.MetadataRepository,
) error {
	bucketName := "silora-demo"
	objectKey := "reports/2026/quarterly-report.pdf"

	// -----------------------------------------------------------
	// Step 1: Create Bucket
	// -----------------------------------------------------------
	sugar.Infow("step 1: creating bucket", "bucket", bucketName)
	if err := objectStore.CreateBucket(ctx, bucketName); err != nil {
		if !errors.Is(err, storage.ErrAlreadyExists) {
			return fmt.Errorf("create bucket: %w", err)
		}
		sugar.Infow("bucket already exists", "bucket", bucketName)
	}

	// -----------------------------------------------------------
	// Step 2: Generate Object ID + Upload to MinIO
	// -----------------------------------------------------------
	objID := uuid.New().String()
	payload := generatePayload(256 * 1024) // 256 KB

	sugar.Infow("step 2: uploading object to minio",
		"bucket", bucketName,
		"key", objectKey,
		"size", len(payload),
		"object_id", objID,
	)

	uploadInfo, err := objectStore.PutObject(ctx, storage.PutObjectRequest{
		Bucket:      bucketName,
		Key:         objectKey,
		Reader:      bytes.NewReader(payload),
		Size:        int64(len(payload)),
		ContentType: "application/pdf",
		Metadata: map[string]string{
			"uploaded-by": "silora-demo",
			"phase":       "3",
		},
	})
	if err != nil {
		return fmt.Errorf("minio upload: %w", err)
	}
	sugar.Infow("minio upload success",
		"etag", uploadInfo.ETag,
		"size", uploadInfo.Size,
	)

	// -----------------------------------------------------------
	// Step 3: Persist Metadata to Pebble (via Cache Decorator)
	// -----------------------------------------------------------
	now := time.Now().UTC()
	meta := &metadata.ObjectMetadata{
		ID:          objID,
		Bucket:      bucketName,
		Key:         objectKey,
		Size:        uploadInfo.Size,
		ContentType: "application/pdf",
		ETag:        uploadInfo.ETag,
		Version:     1,
		State:       metadata.StateAvailable,
		CreatedAt:   now,
		UpdatedAt:   now,
		Metadata: map[string]string{
			"author":     "engineering",
			"department": "platform",
			"retention":  "90d",
		},
	}

	sugar.Infow("step 3: persisting metadata",
		"object_id", objID,
		"bucket", bucketName,
		"key", objectKey,
	)

	if err := repo.Create(ctx, meta); err != nil {
		// COMPENSATION: If metadata write fails, delete the orphaned MinIO object.
		sugar.Warnw("metadata persist failed, compensating",
			"error", err,
			"bucket", bucketName,
			"key", objectKey,
		)
		delCtx, delCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer delCancel()
		if delErr := objectStore.DeleteObject(delCtx, bucketName, objectKey); delErr != nil {
			sugar.Errorw("CRITICAL: compensation failed, orphaned object in minio",
				"bucket", bucketName,
				"key", objectKey,
				"error", delErr,
			)
		}
		return fmt.Errorf("metadata persist: %w", err)
	}
	sugar.Infow("metadata persisted successfully", "object_id", objID)

	// -----------------------------------------------------------
	// Step 4: Read Metadata (Cache MISS → Pebble → Populate Cache)
	// -----------------------------------------------------------
	sugar.Info("step 4: reading metadata (expected: cache MISS → pebble → populate)")

	start := time.Now()
	retrievedByID, err := repo.Get(ctx, objID)
	if err != nil {
		return fmt.Errorf("get by id: %w", err)
	}
	sugar.Infow("get by id (first call = cache miss)",
		"object_id", retrievedByID.ID,
		"size", retrievedByID.Size,
		"etag", retrievedByID.ETag,
		"latency", time.Since(start),
	)

	// -----------------------------------------------------------
	// Step 5: Read Metadata Again (Cache HIT)
	// -----------------------------------------------------------
	sugar.Info("step 5: reading metadata again (expected: cache HIT)")

	start = time.Now()
	retrievedAgain, err := repo.Get(ctx, objID)
	if err != nil {
		return fmt.Errorf("get by id (cached): %w", err)
	}
	sugar.Infow("get by id (second call = cache hit)",
		"object_id", retrievedAgain.ID,
		"latency", time.Since(start),
	)

	// -----------------------------------------------------------
	// Step 6: Read by Bucket + Key (Secondary Index Lookup)
	// -----------------------------------------------------------
	sugar.Info("step 6: reading metadata by bucket+key")

	start = time.Now()
	retrievedByKey, err := repo.GetByKey(ctx, bucketName, objectKey)
	if err != nil {
		return fmt.Errorf("get by key: %w", err)
	}
	sugar.Infow("get by bucket+key",
		"object_id", retrievedByKey.ID,
		"latency", time.Since(start),
	)

	// -----------------------------------------------------------
	// Step 7: Download Object from MinIO
	// -----------------------------------------------------------
	sugar.Info("step 7: downloading object from minio")

	reader, err := objectStore.GetObject(ctx, storage.GetObjectRequest{
		Bucket: retrievedByKey.Bucket,
		Key:    retrievedByKey.Key,
	})
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}

	downloaded, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		return fmt.Errorf("read object stream: %w", err)
	}

	if !bytes.Equal(payload, downloaded) {
		return errors.New("data integrity check failed: downloaded payload mismatch")
	}
	sugar.Infow("download verified",
		"size", len(downloaded),
		"integrity", "ok",
	)

	// -----------------------------------------------------------
	// Step 8: Update Metadata
	// -----------------------------------------------------------
	sugar.Info("step 8: updating metadata")

	retrievedByID.Metadata["reviewed"] = "true"
	retrievedByID.UpdatedAt = time.Now().UTC()

	if err := repo.Update(ctx, retrievedByID); err != nil {
		return fmt.Errorf("update metadata: %w", err)
	}

	// Verify the update is reflected (cache should be invalidated + repopulated)
	updated, err := repo.Get(ctx, objID)
	if err != nil {
		return fmt.Errorf("get after update: %w", err)
	}
	if updated.Metadata["reviewed"] != "true" {
		return errors.New("update not reflected: stale cache data returned")
	}
	sugar.Infow("update verified", "object_id", objID, "reviewed", updated.Metadata["reviewed"])

	// -----------------------------------------------------------
	// Step 9: Exists Check
	// -----------------------------------------------------------
	sugar.Info("step 9: checking existence")

	exists, err := repo.Exists(ctx, objID)
	if err != nil {
		return fmt.Errorf("exists: %w", err)
	}
	sugar.Infow("exists check", "object_id", objID, "exists", exists)

	// -----------------------------------------------------------
	// Step 10: Negative Cache (Not Found)
	// -----------------------------------------------------------
	sugar.Info("step 10: testing negative cache (non-existent object)")

	fakeID := uuid.New().String()
	start = time.Now()
	_, err = repo.Get(ctx, fakeID)
	if !errors.Is(err, metadata.ErrMetadataNotFound) {
		return fmt.Errorf("expected not found, got: %w", err)
	}
	sugar.Infow("first not-found lookup (pebble read)",
		"latency", time.Since(start),
	)

	start = time.Now()
	_, err = repo.Get(ctx, fakeID)
	if !errors.Is(err, metadata.ErrMetadataNotFound) {
		return fmt.Errorf("expected not found (negative cache), got: %w", err)
	}
	sugar.Infow("second not-found lookup (negative cache hit)",
		"latency", time.Since(start),
	)

	// -----------------------------------------------------------
	// Step 11: Delete Object + Metadata
	// -----------------------------------------------------------
	sugar.Info("step 11: deletion workflow")

	// Delete from MinIO first
	if err := objectStore.DeleteObject(ctx, bucketName, objectKey); err != nil {
		return fmt.Errorf("delete minio object: %w", err)
	}
	sugar.Infow("minio object deleted", "bucket", bucketName, "key", objectKey)

	// Delete metadata (invalidates cache)
	if err := repo.Delete(ctx, objID); err != nil {
		return fmt.Errorf("delete metadata: %w", err)
	}
	sugar.Infow("metadata deleted", "object_id", objID)

	// Verify deletion
	exists, err = repo.Exists(ctx, objID)
	if err != nil {
		return fmt.Errorf("exists after delete: %w", err)
	}
	if exists {
		return errors.New("object still exists after deletion")
	}
	sugar.Infow("deletion verified", "object_id", objID, "exists", false)

	// -----------------------------------------------------------
	// Step 12: Health Check
	// -----------------------------------------------------------
	sugar.Info("step 12: health check")

	healthCtx, healthCancel := context.WithTimeout(ctx, 5*time.Second)
	defer healthCancel()

	if err := repo.HealthCheck(healthCtx); err != nil {
		return fmt.Errorf("health check: %w", err)
	}
	sugar.Info("health check passed")

	// -----------------------------------------------------------
	// Step 13: Cleanup Bucket
	// -----------------------------------------------------------
	sugar.Info("step 13: cleanup")

	if err := objectStore.DeleteBucket(ctx, bucketName); err != nil {
		sugar.Warnw("bucket cleanup failed (may contain other objects)", "error", err)
	}

	sugar.Info("demo workflow complete")
	return nil
}

// ---------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------

func generatePayload(size int) []byte {
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		// Fallback for environments where crypto/rand is unavailable
		for i := range buf {
			buf[i] = byte(i % 256)
		}
	}
	return buf
}