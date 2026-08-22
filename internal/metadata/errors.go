package metadata

import (
	"errors"
	"fmt"
)

// Sentinel errors for the metadata layer.
var (
	// ErrMetadataNotFound indicates the requested metadata record does not exist.
	ErrMetadataNotFound = errors.New("metadata: not found")

	// ErrMetadataAlreadyExists indicates a create conflict (record already exists).
	ErrMetadataAlreadyExists = errors.New("metadata: already exists")

	// ErrMetadataConflict indicates a version conflict during update.
	ErrMetadataConflict = errors.New("metadata: version conflict")

	// ErrMetadataCorrupted indicates the stored record cannot be decoded.
	ErrMetadataCorrupted = errors.New("metadata: corrupted record")

	// ErrMetadataInvalid indicates the metadata failed validation.
	ErrMetadataInvalid = errors.New("metadata: invalid")

	// ErrMetadataTooLarge indicates the metadata exceeds size limits.
	ErrMetadataTooLarge = errors.New("metadata: exceeds size limit")

	// ErrIteratorClosed indicates the iterator has been closed.
	ErrIteratorClosed = errors.New("metadata: iterator closed")

	// ErrDatabaseClosed indicates the underlying database is closed.
	ErrDatabaseClosed = errors.New("metadata: database closed")

	// ErrUnsupportedVersion indicates the schema version is not supported.
	ErrUnsupportedVersion = errors.New("metadata: unsupported schema version")
)

// MetadataError wraps a sentinel error with additional context.
type MetadataError struct {
	Op      string
	ID      string
	Bucket  string
	Key     string
	Err     error
}

func (e *MetadataError) Error() string {
	msg := fmt.Sprintf("metadata %s", e.Op)
	if e.ID != "" {
		msg += fmt.Sprintf(" id=%s", e.ID)
	}
	if e.Bucket != "" {
		msg += fmt.Sprintf(" bucket=%s", e.Bucket)
	}
	if e.Key != "" {
		msg += fmt.Sprintf(" key=%s", e.Key)
	}
	if e.Err != nil {
		msg += fmt.Sprintf(": %v", e.Err)
	}
	return msg
}

func (e *MetadataError) Unwrap() error {
	return e.Err
}

// WrapError creates a MetadataError wrapping the given error.
func WrapError(op string, err error) *MetadataError {
	return &MetadataError{Op: op, Err: err}
}