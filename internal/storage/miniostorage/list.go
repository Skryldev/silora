package miniostorage

import (
	"context"

	minio "github.com/minio/minio-go/v7"
	"github.com/Skryldev/silora/internal/storage"
)

func (s *Storage) ListObjects(ctx context.Context, req storage.ListObjectsRequest) (<-chan storage.ListObjectsItem, error) {
	if err := storage.ValidateBucket(req.Bucket); err != nil {
		return nil, err
	}
	if req.MaxKeys < 0 || req.MaxKeys > 1000 {
		return nil, storage.NewError(storage.KindInvalidArgument, "ListObjects", req.Bucket, "", nil)
	}

	const op = "ListObjects"
	ctx, finish, err := s.beginOp(ctx, op)
	if err != nil {
		return nil, err
	}

	out := make(chan storage.ListObjectsItem, 16)

	go func() {
		var finalErr error

		defer func() {
			finish(finalErr)
			close(out)
		}()

		opts := minio.ListObjectsOptions{
			Prefix:    req.Prefix,
			MaxKeys:   req.MaxKeys,
			Recursive: req.Recursive,
		}

		// minio-go v7 uses ListObjectsV2 which uses StartAfter instead of Marker.
		if req.Marker != "" {
			opts.StartAfter = req.Marker
		}

		// Note: minio-go v7 does not expose a custom Delimiter field in ListObjectsOptions.
		// When Recursive is false, it implicitly uses "/" as the delimiter.

		for object := range s.client.ListObjects(ctx, req.Bucket, opts) {
			if object.Err != nil {
				finalErr = mapError(object.Err, op, req.Bucket, "")
				select {
				case out <- storage.ListObjectsItem{Err: finalErr}:
				case <-ctx.Done():
					finalErr = storage.FromContextError(ctx.Err(), op, req.Bucket, "")
				}
				return
			}

			info := toObjectInfo(object, req.Bucket)
			item := storage.ListObjectsItem{Info: &info}

			select {
			case out <- item:
			case <-ctx.Done():
				finalErr = storage.FromContextError(ctx.Err(), op, req.Bucket, "")
				return
			}
		}
	}()

	return out, nil
}