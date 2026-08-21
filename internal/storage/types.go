package storage

import (
	"context"
	"io"
	"sync"
	"time"
)

type BucketInfo struct {
	Name    string
	Created time.Time
}

type ObjectInfo struct {
	Bucket       string
	Key          string
	Size         int64
	ETag         string
	ContentType  string
	VersionID    string
	LastModified time.Time
	Metadata     map[string]string
}

type PutObjectRequest struct {
	Bucket      string
	Key         string
	Reader      io.Reader
	Size        int64
	ContentType string
	Metadata    map[string]string
}

type GetObjectRequest struct {
	Bucket    string
	Key       string
	Offset    int64
	Length    int64
	VersionID string
}

type ListObjectsRequest struct {
	Bucket    string
	Prefix    string
	Delimiter string
	Marker    string
	MaxKeys   int
	Recursive bool
}

type ListObjectsItem struct {
	Info *ObjectInfo
	Err  error
}

type MultipartUploadRequest struct {
	Bucket      string
	Key         string
	Reader      io.Reader
	Size        int64
	ContentType string
	Metadata    map[string]string

	// PartSize and Concurrency override implementation defaults when > 0.
	PartSize    int64
	Concurrency int
}

type HealthStatus struct {
	Healthy bool
	Latency time.Duration
	Error   error
	Message string
}

// ObjectStorage is the primary storage abstraction.
type ObjectStorage interface {
	CreateBucket(ctx context.Context, bucket string) error
	DeleteBucket(ctx context.Context, bucket string) error
	BucketExists(ctx context.Context, bucket string) (bool, error)
	ListBuckets(ctx context.Context) ([]BucketInfo, error)

	PutObject(ctx context.Context, req PutObjectRequest) (ObjectInfo, error)
	GetObject(ctx context.Context, req GetObjectRequest) (*ObjectReader, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	ObjectExists(ctx context.Context, bucket, key string) (bool, error)
	StatObject(ctx context.Context, bucket, key string) (ObjectInfo, error)

	ListObjects(ctx context.Context, req ListObjectsRequest) (<-chan ListObjectsItem, error)
}

type MultipartUploader interface {
	UploadMultipart(ctx context.Context, req MultipartUploadRequest) (ObjectInfo, error)
}

type HealthChecker interface {
	CheckHealth(ctx context.Context) HealthStatus
}

// StorageCore is the full Phase 1 storage capability set.
type StorageCore interface {
	ObjectStorage
	MultipartUploader
	HealthChecker

	// Shutdown stops accepting new operations and waits for active operations.
	Shutdown(ctx context.Context) error

	// Close is a convenience wrapper around Shutdown with a default deadline.
	Close() error
}

// ObjectReader is a streaming object reader.
// Callers must always Close it.
type ObjectReader struct {
	rc      io.ReadCloser
	Info    *ObjectInfo
	onClose func(error)
	once    sync.Once
}

func NewObjectReader(rc io.ReadCloser, info *ObjectInfo, onClose func(error)) *ObjectReader {
	return &ObjectReader{
		rc:      rc,
		Info:    info,
		onClose: onClose,
	}
}

func (r *ObjectReader) Read(p []byte) (int, error) {
	if r == nil || r.rc == nil {
		return 0, io.EOF
	}
	return r.rc.Read(p)
}

func (r *ObjectReader) Close() error {
	var err error
	r.once.Do(func() {
		err = r.rc.Close()
		if r.onClose != nil {
			r.onClose(err)
		}
	})
	return err
}