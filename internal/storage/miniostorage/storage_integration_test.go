package miniostorage

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Skryldev/silora/internal/storage"
)

func testStorage(tb testing.TB) *Storage {
	tb.Helper()

	endpoint := os.Getenv("STORAGE_MINIO_ENDPOINT")
	accessKey := os.Getenv("STORAGE_MINIO_ACCESS_KEY")
	secretKey := os.Getenv("STORAGE_MINIO_SECRET_KEY")

	if endpoint == "" {
		tb.Skip("set STORAGE_MINIO_ENDPOINT to run MinIO integration tests")
	}

	if accessKey == "" {
		accessKey = "minioadmin"
	}
	if secretKey == "" {
		secretKey = "minioadmin"
	}

	secure := false
	if v := os.Getenv("STORAGE_MINIO_SECURE"); v != "" {
		if parsed, err := strconv.ParseBool(v); err == nil {
			secure = parsed
		}
	}

	cfg := DefaultConfig()
	cfg.Endpoint = endpoint
	cfg.AccessKey = accessKey
	cfg.SecretKey = secretKey
	cfg.Secure = secure
	cfg.Multipart.PartSize = 5 * 1024 * 1024
	cfg.Multipart.Concurrency = 2

	s, err := New(cfg)
	if err != nil {
		tb.Fatalf("create storage: %v", err)
	}

	tb.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})

	return s
}

func uniqueBucket(tb testing.TB) string {
	tb.Helper()
	return fmt.Sprintf("storagecore-test-%d", time.Now().UnixNano())
}

func TestBucketLifecycle(t *testing.T) {
	s := testStorage(t)
	ctx := context.Background()
	bucket := uniqueBucket(t)

	if err := s.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}

	t.Cleanup(func() {
		_ = s.DeleteBucket(context.Background(), bucket)
	})

	exists, err := s.BucketExists(ctx, bucket)
	if err != nil {
		t.Fatalf("bucket exists: %v", err)
	}
	if !exists {
		t.Fatal("expected bucket to exist")
	}

	buckets, err := s.ListBuckets(ctx)
	if err != nil {
		t.Fatalf("list buckets: %v", err)
	}

	found := false
	for _, b := range buckets {
		if b.Name == bucket {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("created bucket not found in list")
	}
}

func TestPutGetStatDeleteObject(t *testing.T) {
	s := testStorage(t)
	ctx := context.Background()
	bucket := uniqueBucket(t)

	if err := s.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	t.Cleanup(func() {
		_ = s.DeleteBucket(context.Background(), bucket)
	})

	key := "hello.txt"
	payload := []byte("streaming storage core test payload")

	putInfo, err := s.PutObject(ctx, storage.PutObjectRequest{
		Bucket:      bucket,
		Key:         key,
		Reader:      bytes.NewReader(payload),
		Size:        int64(len(payload)),
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatalf("put object: %v", err)
	}
	if putInfo.Size != int64(len(payload)) {
		t.Fatalf("expected size %d, got %d", len(payload), putInfo.Size)
	}

	exists, err := s.ObjectExists(ctx, bucket, key)
	if err != nil {
		t.Fatalf("object exists: %v", err)
	}
	if !exists {
		t.Fatal("expected object to exist")
	}

	stat, err := s.StatObject(ctx, bucket, key)
	if err != nil {
		t.Fatalf("stat object: %v", err)
	}
	if stat.Size != int64(len(payload)) {
		t.Fatalf("expected stat size %d, got %d", len(payload), stat.Size)
	}

	reader, err := s.GetObject(ctx, storage.GetObjectRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	defer reader.Close()

	downloaded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read object: %v", err)
	}
	if !bytes.Equal(payload, downloaded) {
		t.Fatal("downloaded payload mismatch")
	}

	if err := s.DeleteObject(ctx, bucket, key); err != nil {
		t.Fatalf("delete object: %v", err)
	}

	exists, err = s.ObjectExists(ctx, bucket, key)
	if err != nil {
		t.Fatalf("object exists after delete: %v", err)
	}
	if exists {
		t.Fatal("expected object to be deleted")
	}
}

type zeroReader struct {
	remaining int64
}

var zeroChunk = make([]byte, 64*1024)

func (z *zeroReader) Read(p []byte) (int, error) {
	if z.remaining <= 0 {
		return 0, io.EOF
	}

	n := len(p)
	if int64(n) > z.remaining {
		n = int(z.remaining)
	}
	if n > len(zeroChunk) {
		n = len(zeroChunk)
	}

	copy(p[:n], zeroChunk)
	z.remaining -= int64(n)
	return n, nil
}

func TestRandomObjectRoundTrip(t *testing.T) {
	s := testStorage(t)
	ctx := context.Background()
	bucket := uniqueBucket(t)

	if err := s.CreateBucket(ctx, bucket); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	t.Cleanup(func() {
		_ = s.DeleteBucket(context.Background(), bucket)
	})

	size := int64(128 * 1024)
	payload := make([]byte, size)
	if _, err := rand.Read(payload); err != nil {
		t.Fatalf("generate payload: %v", err)
	}

	key := "random.bin"

	_, err := s.PutObject(ctx, storage.PutObjectRequest{
		Bucket: bucket,
		Key:    key,
		Reader: bytes.NewReader(payload),
		Size:   size,
	})
	if err != nil {
		t.Fatalf("put random object: %v", err)
	}

	reader, err := s.GetObject(ctx, storage.GetObjectRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		t.Fatalf("get random object: %v", err)
	}
	defer reader.Close()

	downloaded, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read random object: %v", err)
	}

	if !bytes.Equal(payload, downloaded) {
		t.Fatal("random payload mismatch")
	}
}