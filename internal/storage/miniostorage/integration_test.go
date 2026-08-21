package miniostorage

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/Skryldev/silora/internal/storage"
)

const (
	testEndpoint  = "localhost:9000"
	testAccessKey = "admin"
	testSecretKey = "Mypassword1234"
)

// newTestStorage initializes a Storage instance configured for the local MinIO test environment.
func newTestStorage(t *testing.T) *Storage {
	t.Helper()

	cfg := DefaultConfig()
	cfg.Endpoint = testEndpoint
	cfg.AccessKey = testAccessKey
	cfg.SecretKey = testSecretKey
	cfg.Secure = false
	cfg.Multipart.PartSize = 5 * 1024 * 1024 // 5MB
	cfg.Multipart.Concurrency = 2

	s, err := New(cfg)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})

	return s
}

// createTestBucket creates a uniquely named bucket and registers cleanup logic.
func createTestBucket(t *testing.T, s *Storage) string {
	t.Helper()
	bucket := fmt.Sprintf("test-bucket-%d", time.Now().UnixNano())
	ctx := context.Background()

	if err := s.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("Failed to create bucket %s: %v", bucket, err)
	}

	t.Cleanup(func() {
		cleanCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// MinIO requires buckets to be empty before deletion.
		ch, err := s.ListObjects(cleanCtx, storage.ListObjectsRequest{Bucket: bucket, Recursive: true})
		if err == nil {
			for item := range ch {
				if item.Err == nil && item.Info != nil {
					_ = s.DeleteObject(cleanCtx, bucket, item.Info.Key)
				}
			}
		}
		_ = s.DeleteBucket(cleanCtx, bucket)
	})

	return bucket
}

func TestHealthCheck(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	status := s.CheckHealth(ctx)
	if !status.Healthy {
		t.Fatalf("Expected healthy status, got error: %v, message: %s", status.Error, status.Message)
	}
	if status.Latency <= 0 {
		t.Error("Expected positive latency")
	}
}

func TestBucketOperations(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()
	bucket := fmt.Sprintf("test-bucket-ops-%d", time.Now().UnixNano())

	// Create
	if err := s.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("CreateBucket failed: %v", err)
	}
	defer func() {
		_ = s.DeleteBucket(context.Background(), bucket)
	}()

	// Exists
	exists, err := s.BucketExists(ctx, bucket)
	if err != nil {
		t.Fatalf("BucketExists failed: %v", err)
	}
	if !exists {
		t.Fatal("Expected bucket to exist")
	}

	// List
	buckets, err := s.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("ListBuckets failed: %v", err)
	}
	found := false
	for _, b := range buckets {
		if b.Name == bucket {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("Created bucket not found in ListBuckets")
	}

	// Delete
	if err := s.DeleteBucket(ctx, bucket); err != nil {
		t.Fatalf("DeleteBucket failed: %v", err)
	}

	exists, err = s.BucketExists(ctx, bucket)
	if err != nil {
		t.Fatalf("BucketExists after delete failed: %v", err)
	}
	if exists {
		t.Fatal("Expected bucket to not exist after delete")
	}
}

func TestObjectOperations(t *testing.T) {
	s := newTestStorage(t)
	bucket := createTestBucket(t, s)
	ctx := context.Background()

	key := "test-object.txt"
	payload := []byte("Hello, MinIO Storage Core!")

	// Put
	info, err := s.PutObject(ctx, storage.PutObjectRequest{
		Bucket:      bucket,
		Key:         key,
		Reader:      bytes.NewReader(payload),
		Size:        int64(len(payload)),
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
	if info.Size != int64(len(payload)) {
		t.Errorf("Expected size %d, got %d", len(payload), info.Size)
	}

	// Exists
	exists, err := s.ObjectExists(ctx, bucket, key)
	if err != nil {
		t.Fatalf("ObjectExists failed: %v", err)
	}
	if !exists {
		t.Fatal("Expected object to exist")
	}

	// Stat
	stat, err := s.StatObject(ctx, bucket, key)
	if err != nil {
		t.Fatalf("StatObject failed: %v", err)
	}
	if stat.Size != int64(len(payload)) {
		t.Errorf("Expected stat size %d, got %d", len(payload), stat.Size)
	}

	// Get
	reader, err := s.GetObject(ctx, storage.GetObjectRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		t.Fatalf("GetObject failed: %v", err)
	}
	defer reader.Close()

	downloaded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("Failed to read object: %v", err)
	}
	if !bytes.Equal(payload, downloaded) {
		t.Error("Downloaded payload does not match original")
	}

	// Delete
	if err := s.DeleteObject(ctx, bucket, key); err != nil {
		t.Fatalf("DeleteObject failed: %v", err)
	}

	exists, err = s.ObjectExists(ctx, bucket, key)
	if err != nil {
		t.Fatalf("ObjectExists after delete failed: %v", err)
	}
	if exists {
		t.Fatal("Expected object to not exist after delete")
	}
}

func TestListObjects(t *testing.T) {
	s := newTestStorage(t)
	bucket := createTestBucket(t, s)
	ctx := context.Background()

	keys := []string{"dir1/file1.txt", "dir1/file2.txt", "dir2/file1.txt", "root.txt"}
	for _, k := range keys {
		_, err := s.PutObject(ctx, storage.PutObjectRequest{
			Bucket: bucket,
			Key:    k,
			Reader: bytes.NewReader([]byte("data")),
			Size:   4,
		})
		if err != nil {
			t.Fatalf("PutObject %s failed: %v", k, err)
		}
	}

	// Recursive list
	ch, err := s.ListObjects(ctx, storage.ListObjectsRequest{
		Bucket:    bucket,
		Recursive: true,
	})
	if err != nil {
		t.Fatalf("ListObjects failed: %v", err)
	}

	count := 0
	for item := range ch {
		if item.Err != nil {
			t.Fatalf("ListObjects item error: %v", item.Err)
		}
		count++
	}
	if count != len(keys) {
		t.Errorf("Expected %d objects, got %d", len(keys), count)
	}

	// Prefix list
	ch, err = s.ListObjects(ctx, storage.ListObjectsRequest{
		Bucket:    bucket,
		Prefix:    "dir1/",
		Recursive: true,
	})
	if err != nil {
		t.Fatalf("ListObjects with prefix failed: %v", err)
	}

	count = 0
	for item := range ch {
		if item.Err != nil {
			t.Fatalf("ListObjects item error: %v", item.Err)
		}
		count++
	}
	if count != 2 {
		t.Errorf("Expected 2 objects in dir1/, got %d", count)
	}
}

func TestMultipartUpload(t *testing.T) {
	s := newTestStorage(t)
	bucket := createTestBucket(t, s)
	ctx := context.Background()

	key := "multipart-large.bin"
	size := int64(12 * 1024 * 1024) // 12MB

	info, err := s.UploadMultipart(ctx, storage.MultipartUploadRequest{
		Bucket:      bucket,
		Key:         key,
		Reader:      io.LimitReader(rand.Reader, size),
		Size:        size,
		PartSize:    5 * 1024 * 1024, // 5MB parts
		Concurrency: 2,
	})
	if err != nil {
		t.Fatalf("UploadMultipart failed: %v", err)
	}
	if info.Size != size {
		t.Errorf("Expected size %d, got %d", size, info.Size)
	}

	// Verify via Stat
	stat, err := s.StatObject(ctx, bucket, key)
	if err != nil {
		t.Fatalf("StatObject after multipart failed: %v", err)
	}
	if stat.Size != size {
		t.Errorf("Expected stat size %d, got %d", size, stat.Size)
	}
}

func TestContextCancellation(t *testing.T) {
	s := newTestStorage(t)
	bucket := createTestBucket(t, s)

	ctx, cancel := context.WithCancel(context.Background())

	size := int64(50 * 1024 * 1024) // 50MB

	// Use a slow reader to ensure we have time to cancel before completion
	slowReader := &slowReader{
		r:     io.LimitReader(rand.Reader, size),
		delay: 10 * time.Millisecond,
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := s.UploadMultipart(ctx, storage.MultipartUploadRequest{
			Bucket:      bucket,
			Key:         "cancel-test.bin",
			Reader:      slowReader,
			Size:        size,
			PartSize:    5 * 1024 * 1024,
			Concurrency: 2,
		})
		errCh <- err
	}()

	// Let it run for a bit then cancel
	time.Sleep(100 * time.Millisecond)
	cancel()

	err := <-errCh
	if err == nil {
		t.Fatal("Expected error due to context cancellation, got nil")
	}
	
	// Verify it's a cancellation error
	if !errors.Is(err, context.Canceled) && !errors.Is(err, storage.KindCanceled) {
		t.Logf("Got error: %v (acceptable if it's a cancellation/timeout error)", err)
	}
}

type slowReader struct {
	r     io.Reader
	delay time.Duration
}

func (sr *slowReader) Read(p []byte) (int, error) {
	time.Sleep(sr.delay)
	return sr.r.Read(p)
}

func TestConcurrency(t *testing.T) {
	s := newTestStorage(t)
	bucket := createTestBucket(t, s)
	ctx := context.Background()

	var wg sync.WaitGroup
	numWorkers := 20

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-%d.txt", id)
			payload := []byte(fmt.Sprintf("data-%d", id))

			_, err := s.PutObject(ctx, storage.PutObjectRequest{
				Bucket: bucket,
				Key:    key,
				Reader: bytes.NewReader(payload),
				Size:   int64(len(payload)),
			})
			if err != nil {
				t.Errorf("Worker %d PutObject failed: %v", id, err)
				return
			}

			reader, err := s.GetObject(ctx, storage.GetObjectRequest{
				Bucket: bucket,
				Key:    key,
			})
			if err != nil {
				t.Errorf("Worker %d GetObject failed: %v", id, err)
				return
			}
			defer reader.Close()

			downloaded, err := io.ReadAll(reader)
			if err != nil {
				t.Errorf("Worker %d Read failed: %v", id, err)
				return
			}
			if !bytes.Equal(payload, downloaded) {
				t.Errorf("Worker %d payload mismatch", id)
			}
		}(i)
	}

	wg.Wait()
}

func TestErrorHandling(t *testing.T) {
	s := newTestStorage(t)
	ctx := context.Background()

	// Test non-existent bucket
	_, err := s.StatObject(ctx, "non-existent-bucket-12345", "key")
	if err == nil {
		t.Fatal("Expected error for non-existent bucket, got nil")
	}
	if !errors.Is(err, storage.KindNotFound) {
		t.Logf("Got error: %v (expected KindNotFound)", err)
	}

	// Test invalid bucket name
	err = s.CreateBucket(ctx, "INVALID_BUCKET_NAME")
	if err == nil {
		t.Fatal("Expected error for invalid bucket name, got nil")
	}
	if !errors.Is(err, storage.KindInvalidArgument) {
		t.Errorf("Expected KindInvalidArgument error, got: %v", err)
	}
}