package storage

import (
	"errors"
	"regexp"
	"strings"
)

var bucketNameRegex = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$`)

// ValidateBucket enforces conservative S3-compatible bucket naming.
func ValidateBucket(name string) error {
	if name == "" {
		return NewError(KindInvalidArgument, "ValidateBucket", name, "", errors.New("bucket name is empty"))
	}

	if len(name) < 3 || len(name) > 63 {
		return NewError(KindInvalidArgument, "ValidateBucket", name, "", errors.New("bucket name must be between 3 and 63 characters"))
	}

	if strings.Contains(name, "..") {
		return NewError(KindInvalidArgument, "ValidateBucket", name, "", errors.New("bucket name contains path traversal sequence"))
	}

	if !bucketNameRegex.MatchString(name) {
		return NewError(KindInvalidArgument, "ValidateBucket", name, "", errors.New("bucket name contains invalid characters or format"))
	}

	return nil
}

// ValidateObjectKey validates logical S3 object keys.
// It intentionally does not impose filesystem semantics.
func ValidateObjectKey(key string) error {
	if key == "" {
		return NewError(KindInvalidArgument, "ValidateObjectKey", "", key, errors.New("object key is empty"))
	}

	if len(key) > 1024 {
		return NewError(KindInvalidArgument, "ValidateObjectKey", "", key, errors.New("object key exceeds 1024 bytes"))
	}

	if strings.ContainsRune(key, 0) {
		return NewError(KindInvalidArgument, "ValidateObjectKey", "", key, errors.New("object key contains null byte"))
	}

	return nil
}

func ValidateMetadata(md map[string]string) error {
	total := 0
	for k, v := range md {
		if k == "" {
			return NewError(KindInvalidArgument, "ValidateMetadata", "", "", errors.New("metadata key is empty"))
		}
		if strings.ContainsRune(k, 0) || strings.ContainsRune(v, 0) {
			return NewError(KindInvalidArgument, "ValidateMetadata", "", "", errors.New("metadata contains null byte"))
		}

		total += len(k) + len(v)
		if total > 16*1024 {
			return NewError(KindInvalidArgument, "ValidateMetadata", "", "", errors.New("metadata exceeds 16 KiB"))
		}
	}

	return nil
}

func ValidateSize(size int64) error {
	if size < -1 {
		return NewError(KindInvalidArgument, "ValidateSize", "", "", errors.New("size must be -1 for unknown or >= 0"))
	}
	return nil
}