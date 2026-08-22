package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/Skryldev/silora/internal/metadata"
	pebblemeta "github.com/Skryldev/silora/internal/metadata/pebble"
	"github.com/Skryldev/silora/internal/storage"
	"github.com/Skryldev/silora/internal/storage/miniostorage"
)

func main() {
	// Initialize structured logger
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		sugar.Info("shutdown signal received")
		cancel()
	}()

	// --- Initialize Phase 1: MinIO Storage Core ---
	minioCfg := miniostorage.ConfigFromEnv()
	if minioCfg.Endpoint == "" {
		minioCfg.Endpoint = "localhost:9000"
	}
	if minioCfg.AccessKey == "" {
		minioCfg.AccessKey = "admin"
	}
	if minioCfg.SecretKey == "" {
		minioCfg.SecretKey = "Mypassword1234"
	}
	minioCfg.Secure = false

	objectStore, err := miniostorage.New(minioCfg)
	if err != nil {
		log.Fatalf("failed to initialize minio storage: %v", err)
	}
	defer objectStore.Close()

	// --- Initialize Phase 2: Pebble Metadata Layer ---
	pebbleCfg := pebblemeta.DefaultConfig("./data/metadata")
	pebbleCfg.WALSync = true // Production durability

	metaRepo, err := pebblemeta.NewPebbleMetadataRepository(pebbleCfg, logger)
	if err != nil {
		log.Fatalf("failed to initialize pebble metadata store: %v", err)
	}
	defer metaRepo.Close()

	// --- Application Workflow ---
	bucketName := "dstore-demo"
	objectKey := "documents/report.pdf"
	payload := []byte("This is the actual file content stored in MinIO.")

	// 1. Ensure bucket exists in MinIO
	if err := objectStore.CreateBucket(ctx, bucketName); err != nil {
		// Ignore if already exists (idempotent in our Phase 1 impl)
		if !errors.Is(err, storage.ErrAlreadyExists) {
			sugar.Fatalf("failed to create bucket: %v", err)
		}
	}

	// 2. Generate Object ID
	objID := uuid.New().String()

	// 3. Upload to MinIO
	sugar.Infof("uploading object to MinIO: %s/%s", bucketName, objectKey)
	uploadInfo, err := objectStore.PutObject(ctx, storage.PutObjectRequest{
		Bucket:      bucketName,
		Key:         objectKey,
		Reader:      bytes.NewReader(payload),
		Size:        int64(len(payload)),
		ContentType: "application/pdf",
	})
	if err != nil {
		sugar.Fatalf("minio upload failed: %v", err)
	}
	sugar.Infof("minio upload success. ETag: %s, Size: %d", uploadInfo.ETag, uploadInfo.Size)

	// 4. Persist Metadata to Pebble
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
			"uploaded_by": "system",
			"phase":       "2",
		},
	}

	sugar.Infof("persisting metadata to Pebble for ID: %s", objID)
	if err := metaRepo.Create(ctx, meta); err != nil {
		sugar.Errorf("pebble metadata write failed: %v", err)
		
		// COMPENSATION: If metadata write fails, we MUST clean up the orphaned MinIO object.
		sugar.Warn("metadata write failed, initiating compensation (deleting MinIO object)")
		delCtx, delCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer delCancel()
		
		if delErr := objectStore.DeleteObject(delCtx, bucketName, objectKey); delErr != nil {
			sugar.Errorf("CRITICAL: compensation failed! Orphaned object in MinIO: %s/%s. Error: %v", 
				bucketName, objectKey, delErr)
		} else {
			sugar.Info("compensation successful. MinIO object deleted.")
		}
		os.Exit(1)
	}

	sugar.Info("object successfully stored with metadata consistency.")

	// 5. Read Metadata and Download
	sugar.Infof("retrieving metadata for bucket=%s key=%s", bucketName, objectKey)
	retrievedMeta, err := metaRepo.GetByKey(ctx, bucketName, objectKey)
	if err != nil {
		sugar.Fatalf("failed to retrieve metadata: %v", err)
	}

	sugar.Infof("metadata retrieved. ID: %s, Size: %d, ETag: %s", 
		retrievedMeta.ID, retrievedMeta.Size, retrievedMeta.ETag)

	// Download from MinIO using the metadata
	reader, err := objectStore.GetObject(ctx, storage.GetObjectRequest{
		Bucket: retrievedMeta.Bucket,
		Key:    retrievedMeta.Key,
	})
	if err != nil {
		sugar.Fatalf("failed to get object from minio: %v", err)
	}
	defer reader.Close()

	downloaded, err := io.ReadAll(reader)
	if err != nil {
		sugar.Fatalf("failed to read object stream: %v", err)
	}

	if !bytes.Equal(payload, downloaded) {
		sugar.Fatal("data integrity check failed: downloaded payload does not match original")
	}
	sugar.Info("data integrity verified. Download successful.")

	// 6. Deletion Workflow
	sugar.Info("initiating deletion workflow...")
	
	// Delete from MinIO first
	if err := objectStore.DeleteObject(ctx, bucketName, objectKey); err != nil {
		sugar.Errorf("failed to delete from minio: %v", err)
		// In a real system, we might mark metadata as StateDeleting and run a reconciler.
	}

	// Delete metadata
	if err := metaRepo.Delete(ctx, retrievedMeta.ID); err != nil {
		sugar.Errorf("failed to delete metadata: %v", err)
	}

	sugar.Info("deletion workflow complete.")
	sugar.Info("Phase 1 + Phase 2 integration demonstration finished successfully.")
}