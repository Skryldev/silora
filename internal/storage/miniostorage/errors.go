package miniostorage

import (
	"context"
	"errors"
	"net"
	"net/url"

	minio "github.com/minio/minio-go/v7"
	"github.com/Skryldev/silora/internal/storage"
)

func mapError(err error, op, bucket, key string) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, context.Canceled) {
		return storage.NewError(storage.KindCanceled, op, bucket, key, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return storage.NewError(storage.KindTimeout, op, bucket, key, err)
	}

	apiErr := minio.ToErrorResponse(err)
	if apiErr.Code != "" || apiErr.StatusCode != 0 {
		kind := kindFromMinio(apiErr.Code, apiErr.StatusCode)
		return storage.NewError(kind, op, bucket, key, err)
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			return storage.NewError(storage.KindTimeout, op, bucket, key, err)
		}
		return storage.NewError(storage.KindUnavailable, op, bucket, key, err)
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return storage.NewError(storage.KindTimeout, op, bucket, key, err)
		}
		return storage.NewError(storage.KindUnavailable, op, bucket, key, err)
	}

	return storage.NewError(storage.KindInternal, op, bucket, key, err)
}

func kindFromMinio(code string, statusCode int) storage.Kind {
	switch code {
	case "NoSuchKey", "NoSuchObject", "NoSuchBucket", "NoSuchUpload":
		return storage.KindNotFound

	case "BucketAlreadyOwnedByYou", "BucketAlreadyExists":
		return storage.KindAlreadyExists

	case "InvalidBucketName",
		"InvalidObjectName",
		"InvalidArgument",
		"InvalidPart",
		"InvalidPartOrder",
		"MalformedXML",
		"EntityTooSmall",
		"KeyTooLong",
		"MethodNotAllowed",
		"RequestTimeTooSkewed":
		return storage.KindInvalidArgument

	case "EntityTooLarge":
		return storage.KindTooLarge

	case "AccessDenied":
		return storage.KindForbidden

	case "InvalidAccessKeyId",
		"SignatureDoesNotMatch",
		"ExpiredToken",
		"InvalidToken",
		"AccountProblem":
		return storage.KindUnauthorized

	case "SlowDown":
		return storage.KindThrottled

	case "ServiceUnavailable",
		"XMinioServerNotInitialized",
		"XMinioReadQuorum",
		"XMinioWriteQuorum":
		return storage.KindUnavailable

	case "InternalError":
		return storage.KindServerFailure

	case "Conflict":
		return storage.KindConflict
	}

	switch statusCode {
	case 404:
		return storage.KindNotFound
	case 408:
		return storage.KindTimeout
	case 429:
		return storage.KindThrottled
	case 400:
		return storage.KindInvalidArgument
	case 401:
		return storage.KindUnauthorized
	case 403:
		return storage.KindForbidden
	case 409:
		return storage.KindConflict
	case 500:
		return storage.KindServerFailure
	case 502, 503, 504:
		return storage.KindUnavailable
	default:
		return storage.KindInternal
	}
}

func isNoSuchUpload(err error) bool {
	if err == nil {
		return false
	}
	return minio.ToErrorResponse(err).Code == "NoSuchUpload"
}