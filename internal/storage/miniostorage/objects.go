package miniostorage

import (
	"bytes"
	"context"
	"errors"
	"io"

	minio "github.com/minio/minio-go/v7"
	"github.com/Skryldev/silora/internal/storage"
)

func (s *Storage) PutObject(ctx context.Context, req storage.PutObjectRequest) (info storage.ObjectInfo, retErr error) {
	if err := storage.ValidateBucket(req.Bucket); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := storage.ValidateObjectKey(req.Key); err != nil {
		return storage.ObjectInfo{}, err
	}
	if req.Reader == nil {
		return storage.ObjectInfo{}, storage.NewError(storage.KindInvalidArgument, "PutObject", req.Bucket, req.Key, errors.New("reader is nil"))
	}
	if err := storage.ValidateSize(req.Size); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := storage.ValidateMetadata(req.Metadata); err != nil {
		return storage.ObjectInfo{}, err
	}

	const op = "PutObject"
	ctx, finish, err := s.beginOp(ctx, op)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	defer func() { finish(retErr) }()

	opts := minio.PutObjectOptions{
		ContentType:            req.ContentType,
		UserMetadata:           req.Metadata,
		PartSize:               uint64(s.cfg.Multipart.PartSize),
		DisableContentSha256:   !s.cfg.Integrity.RequirePayloadSHA256,
	}

	upload := func(ctx context.Context) error {
		uploadInfo, err := s.client.PutObject(ctx, req.Bucket, req.Key, req.Reader, req.Size, opts)
		if err != nil {
			return mapError(err, op, req.Bucket, req.Key)
		}

		size := uploadInfo.Size
		if size == 0 && req.Size > 0 {
			size = req.Size
		}

		info = storage.ObjectInfo{
			Bucket:      uploadInfo.Bucket,
			Key:         uploadInfo.Key,
			Size:        size,
			ETag:        trimETag(uploadInfo.ETag),
			ContentType: req.ContentType,
			VersionID:   uploadInfo.VersionID,
			Metadata:    req.Metadata,
		}

		return nil
	}

	// Retry is only safe if the reader can be reset.
	if rs, ok := req.Reader.(io.ReadSeeker); ok && req.Size >= 0 {
		retErr = s.retry.Do(ctx, op, func(ctx context.Context) error {
			if _, err := rs.Seek(0, io.SeekStart); err != nil {
				return storage.NewError(storage.KindInvalidArgument, op, req.Bucket, req.Key, err)
			}
			return upload(ctx)
		})
	} else {
		retErr = upload(ctx)
	}

	if retErr == nil {
		s.metrics.AddUploadBytes(info.Size)
	}

	return info, retErr
}

func (s *Storage) GetObject(ctx context.Context, req storage.GetObjectRequest) (_ *storage.ObjectReader, retErr error) {
	if err := storage.ValidateBucket(req.Bucket); err != nil {
		return nil, err
	}
	if err := storage.ValidateObjectKey(req.Key); err != nil {
		return nil, err
	}
	if req.Offset < 0 || req.Length < 0 {
		return nil, storage.NewError(storage.KindInvalidArgument, "GetObject", req.Bucket, req.Key, errors.New("offset and length must be >= 0"))
	}

	const op = "GetObject"
	ctx, finish, err := s.beginOp(ctx, op)
	if err != nil {
		return nil, err
	}

	var obj *minio.Object

	err = s.retry.Do(ctx, op, func(ctx context.Context) error {
		opts := minio.GetObjectOptions{}

		if req.Offset > 0 || req.Length > 0 {
			end := int64(0)
			if req.Length > 0 {
				end = req.Offset + req.Length - 1
			}
			opts.SetRange(req.Offset, end)
		}

		if req.VersionID != "" {
			opts.VersionID = req.VersionID
		}

		var getErr error
		obj, getErr = s.client.GetObject(ctx, req.Bucket, req.Key, opts)
		if getErr != nil {
			return mapError(getErr, op, req.Bucket, req.Key)
		}

		return nil
	})

	if err != nil {
		finish(err)
		return nil, err
	}

	// Stat forces early detection of NotFound/permission errors and provides metadata.
	minioInfo, err := obj.Stat()
	if err != nil {
		_ = obj.Close()
		mapped := mapError(err, op, req.Bucket, req.Key)
		finish(mapped)
		return nil, mapped
	}

	info := toObjectInfo(minioInfo, req.Bucket)

	rc := &countingReadCloser{
		rc:       obj,
		onChange: s.metrics.AddDownloadBytes,
	}

	reader := storage.NewObjectReader(rc, &info, func(closeErr error) {
		finish(closeErr)
	})

	return reader, nil
}

func (s *Storage) DeleteObject(ctx context.Context, bucket, key string) (retErr error) {
	if err := storage.ValidateBucket(bucket); err != nil {
		return err
	}
	if err := storage.ValidateObjectKey(key); err != nil {
		return err
	}

	const op = "DeleteObject"
	ctx, finish, err := s.beginOp(ctx, op)
	if err != nil {
		return err
	}
	defer func() { finish(retErr) }()

	retErr = s.retry.Do(ctx, op, func(ctx context.Context) error {
		err := s.client.RemoveObject(ctx, bucket, key, minio.RemoveObjectOptions{})
		if err == nil {
			return nil
		}

		mapped := mapError(err, op, bucket, key)
		if errors.Is(mapped, storage.ErrNotFound) {
			return nil
		}

		return mapped
	})

	return retErr
}

func (s *Storage) ObjectExists(ctx context.Context, bucket, key string) (bool, error) {
	_, err := s.StatObject(ctx, bucket, key)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, storage.ErrNotFound) {
		return false, nil
	}

	return false, err
}

func (s *Storage) StatObject(ctx context.Context, bucket, key string) (info storage.ObjectInfo, retErr error) {
	if err := storage.ValidateBucket(bucket); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := storage.ValidateObjectKey(key); err != nil {
		return storage.ObjectInfo{}, err
	}

	const op = "StatObject"
	ctx, finish, err := s.beginOp(ctx, op)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	defer func() { finish(retErr) }()

	retErr = s.retry.Do(ctx, op, func(ctx context.Context) error {
		minioInfo, err := s.client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
		if err != nil {
			return mapError(err, op, bucket, key)
		}

		info = toObjectInfo(minioInfo, bucket)
		return nil
	})

	return info, retErr
}

func (s *Storage) putObjectSimple(
	ctx context.Context,
	bucket, key string,
	data []byte,
	contentType string,
	metadata map[string]string,
) (storage.ObjectInfo, error) {
	return s.PutObject(ctx, storage.PutObjectRequest{
		Bucket:      bucket,
		Key:         key,
		Reader:      bytes.NewReader(data),
		Size:        int64(len(data)),
		ContentType: contentType,
		Metadata:    metadata,
	})
}