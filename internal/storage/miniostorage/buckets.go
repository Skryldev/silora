package miniostorage

import (
	"context"
	"errors"

	minio "github.com/minio/minio-go/v7"
	"github.com/Skryldev/silora/internal/storage"
)

func (s *Storage) CreateBucket(ctx context.Context, bucket string) (retErr error) {
	if err := storage.ValidateBucket(bucket); err != nil {
		return err
	}

	const op = "CreateBucket"
	ctx, finish, err := s.beginOp(ctx, op)
	if err != nil {
		return err
	}
	defer func() { finish(retErr) }()

	retErr = s.retry.Do(ctx, op, func(ctx context.Context) error {
		err := s.client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{
			Region: s.cfg.Region,
		})
		if err == nil {
			return nil
		}

		apiErr := minio.ToErrorResponse(err)

		// If we own it already, treat as success for bootstrap/idempotency.
		if apiErr.Code == "BucketAlreadyOwnedByYou" {
			return nil
		}

		return mapError(err, op, bucket, "")
	})

	return retErr
}

func (s *Storage) DeleteBucket(ctx context.Context, bucket string) (retErr error) {
	if err := storage.ValidateBucket(bucket); err != nil {
		return err
	}

	const op = "DeleteBucket"
	ctx, finish, err := s.beginOp(ctx, op)
	if err != nil {
		return err
	}
	defer func() { finish(retErr) }()

	retErr = s.retry.Do(ctx, op, func(ctx context.Context) error {
		err := s.client.RemoveBucket(ctx, bucket)
		if err == nil {
			return nil
		}

		mapped := mapError(err, op, bucket, "")
		if errors.Is(mapped, storage.ErrNotFound) {
			return nil
		}

		return mapped
	})

	return retErr
}

func (s *Storage) BucketExists(ctx context.Context, bucket string) (exists bool, retErr error) {
	if err := storage.ValidateBucket(bucket); err != nil {
		return false, err
	}

	const op = "BucketExists"
	ctx, finish, err := s.beginOp(ctx, op)
	if err != nil {
		return false, err
	}
	defer func() { finish(retErr) }()

	retErr = s.retry.Do(ctx, op, func(ctx context.Context) error {
		var err error
		exists, err = s.client.BucketExists(ctx, bucket)
		if err != nil {
			return mapError(err, op, bucket, "")
		}
		return nil
	})

	return exists, retErr
}

func (s *Storage) ListBuckets(ctx context.Context) (buckets []storage.BucketInfo, retErr error) {
	const op = "ListBuckets"
	ctx, finish, err := s.beginOp(ctx, op)
	if err != nil {
		return nil, err
	}
	defer func() { finish(retErr) }()

	retErr = s.retry.Do(ctx, op, func(ctx context.Context) error {
		minioBuckets, err := s.client.ListBuckets(ctx)
		if err != nil {
			return mapError(err, op, "", "")
		}

		buckets = make([]storage.BucketInfo, 0, len(minioBuckets))
		for _, b := range minioBuckets {
			buckets = append(buckets, storage.BucketInfo{
				Name:    b.Name,
				Created: b.CreationDate,
			})
		}

		return nil
	})

	return buckets, retErr
}