package metadata

import (
	"context"
	"io"
)

// MetadataRepository defines the storage-independent interface for metadata operations.
// Higher layers must depend on this interface, not on any concrete implementation.
type MetadataRepository interface {
	// Create persists a new metadata record.
	// Returns ErrMetadataAlreadyExists if the ID already exists.
	Create(ctx context.Context, m *ObjectMetadata) error

	// Get retrieves metadata by object ID.
	// Returns ErrMetadataNotFound if the record does not exist.
	Get(ctx context.Context, id string) (*ObjectMetadata, error)

	// GetByKey retrieves metadata by bucket and object key.
	// Returns ErrMetadataNotFound if the record does not exist.
	GetByKey(ctx context.Context, bucket, key string) (*ObjectMetadata, error)

	// Update modifies an existing metadata record.
	// Returns ErrMetadataNotFound if the record does not exist.
	// Returns ErrMetadataConflict if the version does not match.
	Update(ctx context.Context, m *ObjectMetadata) error

	// Delete removes metadata by object ID.
	// Deletion is idempotent: deleting a non-existent record returns nil.
	Delete(ctx context.Context, id string) error

	// Exists checks whether a metadata record exists by ID.
	Exists(ctx context.Context, id string) (bool, error)

	// List returns a streaming iterator over metadata records matching the request.
	// The caller must close the returned iterator when done.
	List(ctx context.Context, req *ListMetadataRequest) (MetadataIterator, error)

	// CreateMany atomically persists multiple metadata records.
	// Returns ErrMetadataAlreadyExists if any ID already exists (entire batch fails).
	CreateMany(ctx context.Context, records []*ObjectMetadata) error

	// DeleteMany atomically removes multiple metadata records by ID.
	// Non-existent IDs are silently skipped.
	DeleteMany(ctx context.Context, ids []string) error

	// Close releases all resources associated with the repository.
	Close() error

	// HealthCheck verifies the underlying storage is operational.
	HealthCheck(ctx context.Context) error
}

// MetadataIterator provides streaming access to metadata records.
// It is NOT safe for concurrent use; each goroutine must use its own iterator.
type MetadataIterator interface {
	// Next advances to the next record. Returns false when exhausted.
	Next() bool

	// Item returns the current metadata record.
	// Only valid after Next() returns true.
	Item() (*ObjectMetadata, error)

	// Cursor returns an opaque token representing the current position.
	// Can be used to resume iteration from this point.
	Cursor() string

	// Err returns any error encountered during iteration.
	Err() error

	// Close releases iterator resources. Must be called when done.
	Close() error
}

// Ensure MetadataIterator satisfies io.Closer for resource management.
var _ io.Closer = (MetadataIterator)(nil)