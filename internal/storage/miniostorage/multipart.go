package miniostorage

import (
	"bytes"
	"context"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	minio "github.com/minio/minio-go/v7"
	"github.com/Skryldev/silora/internal/storage"
)

type partJob struct {
	number int
	data   []byte
}

func (s *Storage) UploadMultipart(ctx context.Context, req storage.MultipartUploadRequest) (info storage.ObjectInfo, retErr error) {
	if err := storage.ValidateBucket(req.Bucket); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := storage.ValidateObjectKey(req.Key); err != nil {
		return storage.ObjectInfo{}, err
	}
	if req.Reader == nil {
		return storage.ObjectInfo{}, storage.NewError(storage.KindInvalidArgument, "UploadMultipart", req.Bucket, req.Key, nil)
	}
	if err := storage.ValidateSize(req.Size); err != nil {
		return storage.ObjectInfo{}, err
	}
	if err := storage.ValidateMetadata(req.Metadata); err != nil {
		return storage.ObjectInfo{}, err
	}

	// Zero-byte objects are not valid multipart uploads.
	if req.Size == 0 {
		return s.putObjectSimple(ctx, req.Bucket, req.Key, nil, req.ContentType, req.Metadata)
	}

	const op = "UploadMultipart"
	ctx, finish, err := s.beginOp(ctx, op)
	if err != nil {
		return storage.ObjectInfo{}, err
	}
	defer func() { finish(retErr) }()

	partSize := choosePartSize(req.Size, req.PartSize, s.cfg.Multipart.PartSize)
	concurrency := chooseConcurrency(req.Concurrency, s.cfg.Multipart.Concurrency, partSize, s.cfg.Multipart.MaxMemory)

	putOpts := minio.PutObjectOptions{
		ContentType:          req.ContentType,
		UserMetadata:         req.Metadata,
		DisableContentSha256: !s.cfg.Integrity.RequirePayloadSHA256,
	}

	uploadID, err := s.core.NewMultipartUpload(ctx, req.Bucket, req.Key, putOpts)
	if err != nil {
		return storage.ObjectInfo{}, mapError(err, op, req.Bucket, req.Key)
	}
	s.metrics.MultipartOperation("init", nil)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var (
		workerErr      error
		workerErrOnce  sync.Once
		partsMu        sync.Mutex
		parts          []minio.CompletePart
		uploadedBytes  atomic.Int64
		abortSuppressed bool
		completed       bool
	)

	setWorkerErr := func(err error) {
		workerErrOnce.Do(func() {
			workerErr = err
			cancel()
		})
	}

	defer func() {
		if uploadID != "" && !abortSuppressed && (!completed || retErr != nil) {
			s.abortMultipart(ctx, req.Bucket, req.Key, uploadID)
		}
	}()

	jobCh := make(chan partJob, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for job := range jobCh {
				if ctx.Err() != nil {
					continue
				}

				part, err := s.uploadPart(ctx, req.Bucket, req.Key, uploadID, job.number, job.data)
				if err != nil {
					setWorkerErr(err)
					continue
				}

				partsMu.Lock()
				parts = append(parts, minio.CompletePart{
					PartNumber: job.number,
					ETag:       part.ETag,
				})
				partsMu.Unlock()

				uploadedBytes.Add(int64(len(job.data)))
			}
		}()
	}

	var readErr error
	partNumber := 1
	empty := true

	for {
		if ctx.Err() != nil {
			readErr = storage.FromContextError(ctx.Err(), op, req.Bucket, req.Key)
			cancel()
			break
		}

		if partNumber > maxParts {
			readErr = storage.NewError(storage.KindTooLarge, op, req.Bucket, req.Key, nil)
			cancel()
			break
		}

		buf := make([]byte, partSize)
		n, rerr := readPart(req.Reader, buf)

		if n == 0 {
			if rerr == io.EOF {
				break
			}
			if rerr != nil {
				readErr = mapError(rerr, op, req.Bucket, req.Key)
				cancel()
				break
			}
			continue
		}

		empty = false

		select {
		case jobCh <- partJob{number: partNumber, data: buf[:n]}:
		case <-ctx.Done():
			readErr = storage.FromContextError(ctx.Err(), op, req.Bucket, req.Key)
		}

		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			readErr = mapError(rerr, op, req.Bucket, req.Key)
			cancel()
			break
		}

		partNumber++
	}

	close(jobCh)
	wg.Wait()

	if workerErr != nil {
		return storage.ObjectInfo{}, workerErr
	}
	if readErr != nil {
		return storage.ObjectInfo{}, readErr
	}

	if empty {
		// The stream ended before any part was uploaded.
		s.abortMultipart(ctx, req.Bucket, req.Key, uploadID)
		abortSuppressed = true

		info, err := s.putObjectSimple(ctx, req.Bucket, req.Key, nil, req.ContentType, req.Metadata)
		if err != nil {
			return storage.ObjectInfo{}, err
		}

		completed = true
		return info, nil
	}

	partsMu.Lock()
	sort.Slice(parts, func(i, j int) bool {
		return parts[i].PartNumber < parts[j].PartNumber
	})
	completeParts := parts
	partsMu.Unlock()

	var uploadInfo minio.UploadInfo

	err = s.retry.Do(ctx, op+".Complete", func(ctx context.Context) error {
		var err error
		uploadInfo, err = s.core.CompleteMultipartUpload(ctx, req.Bucket, req.Key, uploadID, completeParts, putOpts)
		if err != nil {
			return mapError(err, op, req.Bucket, req.Key)
		}
		return nil
	})

	if err != nil {
		return storage.ObjectInfo{}, err
	}

	completed = true
	s.metrics.MultipartOperation("complete", nil)

	size := uploadedBytes.Load()
	if uploadInfo.Size > 0 {
		size = uploadInfo.Size
	}

	return storage.ObjectInfo{
		Bucket:      req.Bucket,
		Key:         req.Key,
		Size:        size,
		ETag:        trimETag(uploadInfo.ETag),
		ContentType: req.ContentType,
		VersionID:   uploadInfo.VersionID,
		Metadata:    req.Metadata,
	}, nil
}

func (s *Storage) uploadPart(
	ctx context.Context,
	bucket, key, uploadID string,
	partNumber int,
	data []byte,
) (minio.ObjectPart, error) {
	var part minio.ObjectPart

	const op = "MultipartPutObjectPart"

	err := s.retry.Do(ctx, op, func(ctx context.Context) error {
		var err error
		part, err = s.core.PutObjectPart(
			ctx,
			bucket,
			key,
			uploadID,
			partNumber,
			bytes.NewReader(data),
			int64(len(data)),
			minio.PutObjectPartOptions{},
		)
		if err != nil {
			return mapError(err, op, bucket, key)
		}
		return nil
	})

	if err != nil {
		s.metrics.MultipartOperation("part", err)
		return minio.ObjectPart{}, err
	}

	s.metrics.MultipartOperation("part", nil)
	return part, nil
}

func (s *Storage) abortMultipart(parent context.Context, bucket, key, uploadID string) {
	if uploadID == "" {
		return
	}

	timeout := s.cfg.Multipart.AbortTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	ctx, cancel := context.WithTimeout(context.WithoutCancel(parent), timeout)
	defer cancel()

	const op = "MultipartAbort"

	err := s.core.AbortMultipartUpload(ctx, bucket, key, uploadID)
	if err != nil && !isNoSuchUpload(err) {
		s.logger.Warn("abort multipart upload failed",
			"op", op,
			"bucket", bucket,
			"key", key,
			"upload_id", uploadID,
			"error", err,
		)
	}

	s.metrics.MultipartOperation("abort", err)
}

func readPart(r io.Reader, buf []byte) (int, error) {
	n, err := io.ReadFull(r, buf)

	// Last part may be shorter than partSize.
	if err == io.ErrUnexpectedEOF {
		return n, nil
	}

	return n, err
}

func choosePartSize(objectSize, requested, configured int64) int64 {
	partSize := configured
	if requested > 0 {
		partSize = requested
	}

	if partSize < minPartSize {
		partSize = minPartSize
	}
	if partSize > maxPartSize {
		partSize = maxPartSize
	}

	if objectSize > 0 {
		minForSize := (objectSize + maxParts - 1) / maxParts
		if partSize < minForSize {
			partSize = minForSize
		}
		if partSize > maxPartSize {
			partSize = maxPartSize
		}
	}

	return partSize
}

func chooseConcurrency(requested, configured int, partSize, maxMemory int64) int {
	concurrency := configured
	if requested > 0 {
		concurrency = requested
	}

	if concurrency < 1 {
		concurrency = 1
	}
	if concurrency > 1000 {
		concurrency = 1000
	}

	if maxMemory > 0 && partSize > 0 {
		maxAllowed := int(maxMemory / partSize)
		if maxAllowed < 1 {
			maxAllowed = 1
		}
		if concurrency > maxAllowed {
			concurrency = maxAllowed
		}
	}

	return concurrency
}