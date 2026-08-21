package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/Skryldev/silora/internal/storage"
	"github.com/Skryldev/silora/internal/storage/miniostorage"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cfg := miniostorage.ConfigFromEnv()

	// Example:
	// STORAGE_MINIO_ENDPOINT=localhost:9000 \
	// STORAGE_MINIO_ACCESS_KEY=minioadmin \
	// STORAGE_MINIO_SECRET_KEY=minioadmin \
	// go run ./cmd/storagecore-demo
	store, err := miniostorage.New(cfg)
	if err != nil {
		log.Fatalf("create storage: %v", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := store.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	bucket := "demo-bucket"
	if err := store.CreateBucket(ctx, bucket); err != nil {
		log.Fatalf("create bucket: %v", err)
	}

	key := "hello.txt"
	payload := []byte("storage core phase 1")

	info, err := store.PutObject(ctx, storage.PutObjectRequest{
		Bucket:      bucket,
		Key:         key,
		Reader:      bytesReader(payload),
		Size:        int64(len(payload)),
		ContentType: "text/plain",
	})
	if err != nil {
		log.Fatalf("put object: %v", err)
	}

	fmt.Printf("uploaded %s/%s etag=%s size=%d\n", bucket, key, info.ETag, info.Size)

	stat, err := store.StatObject(ctx, bucket, key)
	if err != nil {
		log.Fatalf("stat object: %v", err)
	}
	fmt.Printf("stat %s size=%d etag=%s\n", stat.Key, stat.Size, stat.ETag)

	reader, err := store.GetObject(ctx, storage.GetObjectRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		log.Fatalf("get object: %v", err)
	}
	defer reader.Close()

	if _, err := io.Copy(os.Stdout, reader); err != nil {
		log.Fatalf("download object: %v", err)
	}
	fmt.Println()

	health := store.CheckHealth(ctx)
	fmt.Printf("healthy=%v latency=%s\n", health.Healthy, health.Latency)

	if err := store.DeleteObject(ctx, bucket, key); err != nil {
		log.Fatalf("delete object: %v", err)
	}

	if err := store.DeleteBucket(ctx, bucket); err != nil {
		log.Fatalf("delete bucket: %v", err)
	}
}

type sliceReader struct {
	data []byte
	pos  int
}

func bytesReader(data []byte) io.Reader {
	return &sliceReader{data: data}
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}