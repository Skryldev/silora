package metadata

import (
	"time"
)

// ObjectState represents the lifecycle state of an object's metadata.
type ObjectState uint8

const (
	StatePending   ObjectState = iota // Upload initiated, not yet confirmed
	StateAvailable                    // Fully uploaded and metadata persisted
	StateDeleting                     // Deletion in progress
	StateDeleted                      // Soft-deleted, awaiting reconciliation
)

// String returns a human-readable representation of the state.
func (s ObjectState) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateAvailable:
		return "available"
	case StateDeleting:
		return "deleting"
	case StateDeleted:
		return "deleted"
	default:
		return "unknown"
	}
}

// ObjectMetadata is the domain model for object metadata.
// It is independent of both Pebble and MinIO SDK types.
type ObjectMetadata struct {
	ID          string
	Bucket      string
	Key         string
	Size        int64
	ContentType string
	ETag        string
	Checksum    string
	Version     uint64
	State       ObjectState
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Metadata    map[string]string
}

// Validate performs basic validation on the metadata record.
func (m *ObjectMetadata) Validate() error {
	if m.ID == "" {
		return ErrMetadataInvalid
	}
	if m.Bucket == "" {
		return ErrMetadataInvalid
	}
	if m.Key == "" {
		return ErrMetadataInvalid
	}
	if m.Size < 0 {
		return ErrMetadataInvalid
	}
	return nil
}

// ListMetadataRequest defines parameters for listing metadata records.
type ListMetadataRequest struct {
	// Bucket filters results to a specific bucket. Empty means all buckets.
	Bucket string

	// Prefix filters object keys by prefix within the bucket.
	Prefix string

	// Cursor is an opaque pagination token from a previous List call.
	// Empty string means start from the beginning.
	Cursor string

	// Limit is the maximum number of results to return.
	// Zero means use the default limit.
	Limit int

	// IncludeDeleted includes records in StateDeleted if true.
	IncludeDeleted bool
}

// DefaultListLimit is the default page size for list operations.
const DefaultListLimit = 100

// MaxListLimit is the maximum allowed page size.
const MaxListLimit = 10000

// MaxMetadataValueSize is the maximum allowed size for a single metadata value.
const MaxMetadataValueSize = 64 * 1024 // 64 KB

// MaxMetadataEntries is the maximum number of custom metadata entries per object.
const MaxMetadataEntries = 256

// MaxKeyLength is the maximum allowed length for an object key.
const MaxKeyLength = 1024

// MaxBucketLength is the maximum allowed length for a bucket name.
const MaxBucketLength = 255