package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Skryldev/silora/internal/storage"
	"github.com/Skryldev/silora/internal/storage/miniostorage"
)

func main() {
	// 1. Initialize Structured Logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	// 2. Setup Context with OS Signal Handling for Graceful Shutdown
	// This ensures that if the user presses Ctrl+C, the context is cancelled,
	// allowing active uploads/downloads to abort cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 3. Configure Storage
	// In production, use miniostorage.ConfigFromEnv() instead of hardcoding.
	cfg := miniostorage.DefaultConfig()
	cfg.Endpoint = "localhost:9000"
	cfg.AccessKey = "admin"
	cfg.SecretKey = "Mypassword1234"
	cfg.Secure = false
	cfg.Logger = logger
	cfg.Multipart.PartSize = 5 * 1024 * 1024 // 5MB parts
	cfg.Multipart.Concurrency = 4            // 4 concurrent part uploads

	// 4. Initialize Storage Core
	store, err := miniostorage.New(cfg)
	if err != nil {
		logger.Error("failed to initialize storage", "error", err)
		os.Exit(1)
	}

	// 5. Ensure Graceful Shutdown
	// We use a separate background context here because the main `ctx` 
	// might already be cancelled if the program was interrupted by a signal.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		
		logger.Info("initiating storage shutdown...")
		if err := store.Shutdown(shutdownCtx); err != nil {
			logger.Error("storage shutdown error", "error", err)
		} else {
			logger.Info("storage shutdown complete")
		}
	}()

	// 6. Run the Demo Logic
	if err := runDemo(ctx, store, logger); err != nil {
		logger.Error("demo failed", "error", err)
		os.Exit(1)
	}
	
	logger.Info("demo completed successfully")
}

func runDemo(ctx context.Context, store storage.StorageCore, logger *slog.Logger) error {
	bucket := fmt.Sprintf("silora-demo-%d", time.Now().Unix())
	logger.Info("starting demo", "bucket", bucket)

	// --- Health Check ---
	health := store.CheckHealth(ctx)
	if !health.Healthy {
		return fmt.Errorf("storage unhealthy: %v", health.Error)
	}
	logger.Info("health check passed", "latency", health.Latency)

	// --- Create Bucket ---
	if err := store.CreateBucket(ctx, bucket); err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	logger.Info("bucket created", "bucket", bucket)

	// Defer cleanup to ensure we don't leave garbage in MinIO
	defer cleanupBucket(store, bucket, logger)

	// --- Small Object Upload (Streaming) ---
	smallKey := "hello.txt"
	smallPayload := []byte("Hello from Silora Storage Core Phase 1!")
	
	_, err := store.PutObject(ctx, storage.PutObjectRequest{
		Bucket:      bucket,
		Key:         smallKey,
		Reader:      bytes.NewReader(smallPayload),
		Size:        int64(len(smallPayload)),
		ContentType: "text/plain",
	})
	if err != nil {
		return fmt.Errorf("put small object: %w", err)
	}
	logger.Info("small object uploaded", "key", smallKey)

	// --- Large Object Upload (Multipart) ---
	largeKey := "large-data.bin"
	largeSize := int64(12 * 1024 * 1024) // 12MB
	
	logger.Info("starting multipart upload", "key", largeKey, "size_mb", largeSize/(1024*1024))
	_, err = store.UploadMultipart(ctx, storage.MultipartUploadRequest{
		Bucket:      bucket,
		Key:         largeKey,
		Reader:      io.LimitReader(rand.Reader, largeSize), // Streams 12MB of random data
		Size:        largeSize,
		ContentType: "application/octet-stream",
		PartSize:    5 * 1024 * 1024, // 5MB parts
		Concurrency: 2,               // 2 concurrent workers
	})
	if err != nil {
		return fmt.Errorf("multipart upload: %w", err)
	}
	logger.Info("large object uploaded", "key", largeKey)

	// --- List Objects ---
	ch, err := store.ListObjects(ctx, storage.ListObjectsRequest{
		Bucket:    bucket,
		Recursive: true,
	})
	if err != nil {
		return fmt.Errorf("list objects: %w", err)
	}
	
	count := 0
	for item := range ch {
		if item.Err != nil {
			return fmt.Errorf("list item error: %w", item.Err)
		}
		count++
		logger.Info("found object", "key", item.Info.Key, "size", item.Info.Size)
	}
	logger.Info("list complete", "count", count)

	// --- Download & Verify Object ---
	reader, err := store.GetObject(ctx, storage.GetObjectRequest{
		Bucket: bucket,
		Key:    smallKey,
	})
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	
	// Read the stream (in a real app, you would stream this to a file or HTTP response)
	downloaded, err := io.ReadAll(reader)
	reader.Close() // Always close the reader!
	
	if err != nil {
		return fmt.Errorf("read object: %w", err)
	}
	if string(downloaded) != string(smallPayload) {
		return fmt.Errorf("payload mismatch")
	}
	logger.Info("small object downloaded and verified", "key", smallKey)

	return nil
}

func cleanupBucket(store storage.StorageCore, bucket string, logger *slog.Logger) {
	// Use a fresh background context for cleanup, as the main context might be cancelled
	cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	logger.Info("cleaning up bucket", "bucket", bucket)
	
	ch, err := store.ListObjects(cleanCtx, storage.ListObjectsRequest{Bucket: bucket, Recursive: true})
	if err == nil {
		for item := range ch {
			if item.Err == nil && item.Info != nil {
				_ = store.DeleteObject(cleanCtx, bucket, item.Info.Key)
			}
		}
	}
	_ = store.DeleteBucket(cleanCtx, bucket)
}