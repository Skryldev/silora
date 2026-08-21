package miniostorage

import (
	"context"
	"time"

	"github.com/Skryldev/silora/internal/storage"
)

func (s *Storage) CheckHealth(ctx context.Context) storage.HealthStatus {
	start := time.Now()

	if s.closed.Load() {
		return storage.HealthStatus{
			Healthy: false,
			Latency: 0,
			Error:   storage.ErrClosed,
			Message: "storage is closed",
		}
	}

	timeout := s.cfg.Health.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var err error

	if s.cfg.Health.Bucket != "" {
		_, err = s.client.BucketExists(ctx, s.cfg.Health.Bucket)
	} else {
		_, err = s.client.ListBuckets(ctx)
	}

	latency := time.Since(start)

	if err != nil {
		return storage.HealthStatus{
			Healthy: false,
			Latency: latency,
			Error:   mapError(err, "CheckHealth", s.cfg.Health.Bucket, ""),
			Message: "minio health check failed",
		}
	}

	return storage.HealthStatus{
		Healthy: true,
		Latency: latency,
		Message: "minio reachable",
	}
}