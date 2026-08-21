package miniostorage

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/Skryldev/silora/internal/storage"
)

var _ storage.StorageCore = (*Storage)(nil)

type Storage struct {
	cfg       Config
	client    *minio.Client
	core      *minio.Core
	transport *http.Transport
	retry     *storage.Retryer
	metrics   storage.Metrics
	tracer    storage.Tracer
	logger    *slog.Logger

	closed atomic.Bool
	active sync.WaitGroup
}

func New(cfg Config) (*Storage, error) {
	cfg.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	client, transport, err := newMinioClient(cfg)
	if err != nil {
		return nil, err
	}

	retry := storage.NewRetryer(
		cfg.Retry.MaxAttempts,
		cfg.Retry.InitialBackoff,
		cfg.Retry.MaxBackoff,
		cfg.Retry.Multiplier,
		cfg.Metrics,
		cfg.Logger,
	)

	s := &Storage{
		cfg:       cfg,
		client:    client,
		core:      &minio.Core{Client: client},
		transport: transport,
		retry:     retry,
		metrics:   cfg.Metrics,
		tracer:    cfg.Tracer,
		logger:    cfg.Logger.With(
			slog.String("endpoint", cfg.Endpoint),
			slog.Bool("secure", cfg.Secure),
			slog.String("region", cfg.Region),
		),
	}

	return s, nil
}

func (s *Storage) Shutdown(ctx context.Context) error {
	if !s.closed.CompareAndSwap(false, true) {
		return nil
	}

	done := make(chan struct{})
	go func() {
		s.active.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.transport.CloseIdleConnections()
		return nil
	case <-ctx.Done():
		s.transport.CloseIdleConnections()
		return storage.FromContextError(ctx.Err(), "Shutdown", "", "")
	}
}

func (s *Storage) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return s.Shutdown(ctx)
}

func (s *Storage) beginOp(ctx context.Context, op string) (context.Context, func(error), error) {
	if s.closed.Load() {
		return ctx, nil, storage.NewError(storage.KindClosed, op, "", "", nil)
	}

	if err := ctx.Err(); err != nil {
		return ctx, nil, storage.FromContextError(err, op, "", "")
	}

	s.active.Add(1)
	start := time.Now()

	ctx, finishTrace := s.tracer.Start(ctx, op, nil)
	ctx = s.metrics.OperationStarted(ctx, op)

	finish := func(err error) {
		finishTrace(err)
		s.metrics.OperationFinished(ctx, op, err, time.Since(start))
		s.active.Done()
	}

	return ctx, finish, nil
}

func trimETag(etag string) string {
	return strings.Trim(etag, `"`)
}

func toObjectInfo(info minio.ObjectInfo, bucket string) storage.ObjectInfo {
	return storage.ObjectInfo{
		Bucket:       bucket,
		Key:          info.Key,
		Size:         info.Size,
		ETag:         trimETag(info.ETag),
		ContentType:  info.ContentType,
		LastModified: info.LastModified,
		Metadata:     info.UserMetadata,
	}
}

type countingReadCloser struct {
	rc       io.ReadCloser
	onChange func(int64)
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.rc.Read(p)
	if n > 0 && c.onChange != nil {
		c.onChange(int64(n))
	}
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.rc.Close()
}