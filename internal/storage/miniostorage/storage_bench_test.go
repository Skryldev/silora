package miniostorage

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/Skryldev/silora/internal/storage"
)

func benchStorage(b *testing.B) *Storage {
	b.Helper()
	return testStorage(b)
}

func BenchmarkPutObject(b *testing.B) {
	s := benchStorage(b)
	ctx := context.Background()
	bucket := fmt.Sprintf("bench-put-%d", time.Now().UnixNano())

	if err := s.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("create bucket: %v", err)
	}

	b.Cleanup(func() {
		_ = s.DeleteBucket(context.Background(), bucket)
	})

	sizes := []struct {
		name string
		size int64
	}{
		{"1KiB", 1 << 10},
		{"1MiB", 1 << 20},
		{"16MiB", 16 << 20},
	}

	for _, tc := range sizes {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(tc.size)

			for i := 0; i < b.N; i++ {
				key := fmt.Sprintf("bench-%d-%s", i, tc.name)

				_, err := s.PutObject(ctx, storage.PutObjectRequest{
					Bucket: bucket,
					Key:    key,
					Reader: &zeroReader{remaining: tc.size},
					Size:   tc.size,
				})
				if err != nil {
					b.Fatalf("put object: %v", err)
				}
			}
		})
	}
}

func BenchmarkGetObject(b *testing.B) {
	s := benchStorage(b)
	ctx := context.Background()
	bucket := fmt.Sprintf("bench-get-%d", time.Now().UnixNano())

	if err := s.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("create bucket: %v", err)
	}

	b.Cleanup(func() {
		_ = s.DeleteBucket(context.Background(), bucket)
	})

	size := int64(1 << 20)
	key := "bench-get-1mib"

	_, err := s.PutObject(ctx, storage.PutObjectRequest{
		Bucket: bucket,
		Key:    key,
		Reader: &zeroReader{remaining: size},
		Size:   size,
	})
	if err != nil {
		b.Fatalf("seed object: %v", err)
	}

	b.SetBytes(size)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader, err := s.GetObject(ctx, storage.GetObjectRequest{
			Bucket: bucket,
			Key:    key,
		})
		if err != nil {
			b.Fatalf("get object: %v", err)
		}

		if _, err := io.Copy(io.Discard, reader); err != nil {
			_ = reader.Close()
			b.Fatalf("download object: %v", err)
		}

		_ = reader.Close()
	}
}

func BenchmarkMultipartUpload(b *testing.B) {
	s := benchStorage(b)
	ctx := context.Background()
	bucket := fmt.Sprintf("bench-multipart-%d", time.Now().UnixNano())

	if err := s.CreateBucket(ctx, bucket); err != nil {
		b.Fatalf("create bucket: %v", err)
	}

	b.Cleanup(func() {
		_ = s.DeleteBucket(context.Background(), bucket)
	})

	size := int64(32 << 20)

	b.SetBytes(size)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-multipart-%d", i)

		_, err := s.UploadMultipart(ctx, storage.MultipartUploadRequest{
			Bucket:      bucket,
			Key:         key,
			Reader:      &zeroReader{remaining: size},
			Size:        size,
			PartSize:    5 * 1024 * 1024,
			Concurrency: 4,
		})
		if err != nil {
			b.Fatalf("multipart upload: %v", err)
		}
	}
}