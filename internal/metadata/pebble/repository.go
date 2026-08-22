package pebble

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"go.uber.org/zap"

	"github.com/Skryldev/silora/internal/metadata"
)

// ErrDatabaseClosed is returned when operations are attempted on a closed repository.
var ErrDatabaseClosed = metadata.ErrDatabaseClosed

// PebbleMetadataRepository implements metadata.MetadataRepository using Pebble.
type PebbleMetadataRepository struct {
	db        *pebble.DB
	logger    *zap.Logger
	closed    atomic.Bool
	once      sync.Once
	writeOpts *pebble.WriteOptions
}

// Compile-time interface check.
var _ metadata.MetadataRepository = (*PebbleMetadataRepository)(nil)

// NewPebbleMetadataRepository opens or creates a Pebble database at the configured
func NewPebbleMetadataRepository(cfg Config, logger *zap.Logger) (*PebbleMetadataRepository, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid pebble config: %w", err)
	}

	if logger == nil {
		logger = zap.NewNop()
	}

	opts := cfg.toPebbleOptions()
	db, err := pebble.Open(cfg.Path, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble at %s: %w", cfg.Path, err)
	}

	logger.Info("pebble metadata store opened",
		zap.String("path", cfg.Path),
		zap.Int64("cache_size", cfg.CacheSize),
		zap.Bool("wal_sync", cfg.WALSync),
	)

	// Resolve write options based on configuration
	writeOpts := pebble.NoSync
	if cfg.WALSync {
		writeOpts = pebble.Sync
	}

	return &PebbleMetadataRepository{
		db:        db,
		logger:    logger,
		writeOpts: writeOpts,
	}, nil
}

// Create persists a new metadata record atomically (primary + secondary index).
func (r *PebbleMetadataRepository) Create(ctx context.Context, m *metadata.ObjectMetadata) error {
	start := time.Now()
	defer func() { observeWrite(start, nil) }()

	if r.closed.Load() {
		return ErrDatabaseClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := m.Validate(); err != nil {
		return err
	}

	// Check for existing record.
	pk := encodePrimaryKey(m.ID)
	_, closer, err := r.db.Get(pk)
	if err == nil {
		closer.Close()
		return &metadata.MetadataError{Op: "create", ID: m.ID, Err: metadata.ErrMetadataAlreadyExists}
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return fmt.Errorf("create: check existence: %w", err)
	}

	// Encode the metadata value.
	value, err := encodeMetadata(m)
	if err != nil {
		return err
	}

	// Atomic batch: primary record + secondary index.
	sk := encodeSecondaryKey(m.Bucket, m.Key)

	batch := r.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(pk, value, nil); err != nil {
		return fmt.Errorf("create: set primary: %w", err)
	}
	if err := batch.Set(sk, pk, nil); err != nil { // secondary index points to primary key
		return fmt.Errorf("create: set secondary: %w", err)
	}

	if err := batch.Commit(r.writeOpts); err != nil {
		return fmt.Errorf("create: commit: %w", err)
	}

	return nil
}

// Get retrieves metadata by object ID.
func (r *PebbleMetadataRepository) Get(ctx context.Context, id string) (*metadata.ObjectMetadata, error) {
	start := time.Now()
	var result *metadata.ObjectMetadata
	var retErr error
	defer func() { observeRead(start, retErr) }()

	if r.closed.Load() {
		return nil, ErrDatabaseClosed
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	pk := encodePrimaryKey(id)
	value, closer, err := r.db.Get(pk)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			metadataNotFoundTotal.Inc()
			retErr = &metadata.MetadataError{Op: "get", ID: id, Err: metadata.ErrMetadataNotFound}
			return nil, retErr
		}
		retErr = fmt.Errorf("get: %w", err)
		return nil, retErr
	}

	// Decode while value is still valid (before closer.Close()).
	result, err = decodeMetadata(value)
	closer.Close() // Release Pebble-owned memory after decoding.

	if err != nil {
		retErr = &metadata.MetadataError{Op: "get", ID: id, Err: err}
		return nil, retErr
	}

	return result, nil
}

// GetByKey retrieves metadata by bucket and object key using the secondary index.
func (r *PebbleMetadataRepository) GetByKey(ctx context.Context, bucket, key string) (*metadata.ObjectMetadata, error) {
	start := time.Now()
	var result *metadata.ObjectMetadata
	var retErr error
	defer func() { observeRead(start, retErr) }()

	if r.closed.Load() {
		return nil, ErrDatabaseClosed
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Look up secondary index to get primary key.
	sk := encodeSecondaryKey(bucket, key)
	pk, closer, err := r.db.Get(sk)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			metadataNotFoundTotal.Inc()
			retErr = &metadata.MetadataError{Op: "get_by_key", Bucket: bucket, Key: key, Err: metadata.ErrMetadataNotFound}
			return nil, retErr
		}
		retErr = fmt.Errorf("get_by_key: secondary lookup: %w", err)
		return nil, retErr
	}

	// Copy the primary key before closing.
	pkCopy := make([]byte, len(pk))
	copy(pkCopy, pk)
	closer.Close()

	// Fetch the primary record.
	value, closer2, err := r.db.Get(pkCopy)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// Stale secondary index — primary was deleted but index wasn't cleaned up.
			r.logger.Warn("stale secondary index detected",
				zap.String("bucket", bucket),
				zap.String("key", key),
			)
			retErr = &metadata.MetadataError{Op: "get_by_key", Bucket: bucket, Key: key, Err: metadata.ErrMetadataNotFound}
			return nil, retErr
		}
		retErr = fmt.Errorf("get_by_key: primary lookup: %w", err)
		return nil, retErr
	}

	result, err = decodeMetadata(value)
	closer2.Close()

	if err != nil {
		retErr = &metadata.MetadataError{Op: "get_by_key", Bucket: bucket, Key: key, Err: err}
		return nil, retErr
	}

	return result, nil
}

// Update modifies an existing metadata record with optimistic concurrency control.
// The Version field is checked; if it doesn't match, ErrMetadataConflict is returned.
func (r *PebbleMetadataRepository) Update(ctx context.Context, m *metadata.ObjectMetadata) error {
	start := time.Now()
	defer func() { observeWrite(start, nil) }()

	if r.closed.Load() {
		return ErrDatabaseClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := m.Validate(); err != nil {
		return err
	}

	pk := encodePrimaryKey(m.ID)

	// Read existing record for version check.
	existingValue, closer, err := r.db.Get(pk)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return &metadata.MetadataError{Op: "update", ID: m.ID, Err: metadata.ErrMetadataNotFound}
		}
		return fmt.Errorf("update: read existing: %w", err)
	}

	existing, err := decodeMetadata(existingValue)
	closer.Close()

	if err != nil {
		return &metadata.MetadataError{Op: "update", ID: m.ID, Err: err}
	}

	// Optimistic concurrency check.
	if existing.Version != m.Version {
		return &metadata.MetadataError{Op: "update", ID: m.ID, Err: metadata.ErrMetadataConflict}
	}

	// Increment version.
	m.Version = existing.Version + 1

	// Encode updated metadata.
	value, err := encodeMetadata(m)
	if err != nil {
		return err
	}

	// If bucket/key changed, update secondary index atomically.
	batch := r.db.NewBatch()
	defer batch.Close()

	if err := batch.Set(pk, value, nil); err != nil {
		return fmt.Errorf("update: set primary: %w", err)
	}

	// Update secondary index if bucket/key changed.
	oldSK := encodeSecondaryKey(existing.Bucket, existing.Key)
	newSK := encodeSecondaryKey(m.Bucket, m.Key)

	if existing.Bucket != m.Bucket || existing.Key != m.Key {
		if err := batch.Delete(oldSK, nil); err != nil {
			return fmt.Errorf("update: delete old secondary: %w", err)
		}
		if err := batch.Set(newSK, pk, nil); err != nil {
			return fmt.Errorf("update: set new secondary: %w", err)
		}
	}

	if err := batch.Commit(r.writeOpts); err != nil {
		return fmt.Errorf("update: commit: %w", err)
	}

	return nil
}

// Delete removes metadata by object ID.
// Deletion is idempotent: deleting a non-existent record returns nil.
func (r *PebbleMetadataRepository) Delete(ctx context.Context, id string) error {
	start := time.Now()
	defer func() { observeDelete(start, nil) }()

	if r.closed.Load() {
		return ErrDatabaseClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	pk := encodePrimaryKey(id)

	// Read the record to get bucket/key for secondary index cleanup.
	value, closer, err := r.db.Get(pk)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			// Idempotent: already deleted.
			return nil
		}
		return fmt.Errorf("delete: read: %w", err)
	}

	m, err := decodeMetadata(value)
	closer.Close()

	if err != nil {
		// Corrupted record; still attempt to delete primary.
		r.logger.Error("deleting corrupted metadata record",
			zap.String("id", id),
			zap.Error(err),
		)
		return r.db.Delete(pk, r.writeOpts)
	}

	// Atomic deletion of primary + secondary index.
	sk := encodeSecondaryKey(m.Bucket, m.Key)

	batch := r.db.NewBatch()
	defer batch.Close()

	if err := batch.Delete(pk, nil); err != nil {
		return fmt.Errorf("delete: primary: %w", err)
	}
	if err := batch.Delete(sk, nil); err != nil {
		return fmt.Errorf("delete: secondary: %w", err)
	}

	if err := batch.Commit(r.writeOpts); err != nil {
		return fmt.Errorf("delete: commit: %w", err)
	}

	return nil
}

// Exists checks whether a metadata record exists by ID.
func (r *PebbleMetadataRepository) Exists(ctx context.Context, id string) (bool, error) {
	if r.closed.Load() {
		return false, ErrDatabaseClosed
	}

	select {
	case <-ctx.Done():
		return false, ctx.Err()
	default:
	}

	pk := encodePrimaryKey(id)
	_, closer, err := r.db.Get(pk)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("exists: %w", err)
	}
	closer.Close()
	return true, nil
}

// List returns a streaming iterator over metadata records.
func (r *PebbleMetadataRepository) List(ctx context.Context, req *metadata.ListMetadataRequest) (metadata.MetadataIterator, error) {
	if r.closed.Load() {
		return nil, ErrDatabaseClosed
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if req == nil {
		req = &metadata.ListMetadataRequest{}
	}

	limit := req.Limit
	if limit <= 0 {
		limit = metadata.DefaultListLimit
	}
	if limit > metadata.MaxListLimit {
		limit = metadata.MaxListLimit
	}

	metadataIteratorOpsTotal.WithLabelValues("create").Inc()

	// Determine scan bounds.
	var lowerBound, upperBound []byte

	if req.Bucket != "" {
		if req.Prefix != "" {
			lowerBound = encodeBucketKeyPrefix(req.Bucket, req.Prefix)
			// Upper bound: increment the last byte of the prefix.
			upperBound = incrementBytes(lowerBound)
		} else {
			lowerBound = encodeBucketPrefix(req.Bucket)
			upperBound = incrementBytes(encodeSecondaryKey(req.Bucket, ""))
			// Actually, for bucket prefix scan, we want everything starting with the bucket prefix.
			// Use the bucket prefix as lower bound and increment for upper bound.
			lowerBound = encodeBucketPrefix(req.Bucket)
			// Remove the 0xFF suffix for the actual lower bound.
			lowerBound = lowerBound[:len(lowerBound)-1]
			upperBound = encodeBucketPrefix(req.Bucket)
		}
	} else {
		// Scan all secondary keys.
		lowerBound = []byte{keyPrefixSecondary}
		upperBound = []byte{keyPrefixSecondary + 1}
	}

	// Apply cursor if provided.
	if req.Cursor != "" {
		cursorKey, err := decodeCursor(req.Cursor)
		if err != nil {
			return nil, err
		}
		if cursorKey != nil {
			// Start after the cursor key.
			lowerBound = incrementBytes(cursorKey)
		}
	}

	iterOpts := &pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	}

	iter, err := r.db.NewIter(iterOpts)
	if err != nil {
		return nil, fmt.Errorf("list: create iterator: %w", err)
	}

	if !iter.First() {
		iter.Close()
		return &emptyIterator{}, nil
	}
	// Rewind so Next() in the iterator wrapper starts correctly.
	iter.First()

	return newPebbleMetadataIterator(r.db, iter, limit), nil
}

// CreateMany atomically persists multiple metadata records.
func (r *PebbleMetadataRepository) CreateMany(ctx context.Context, records []*metadata.ObjectMetadata) error {
	start := time.Now()
	defer func() {
		metadataBatchOpsTotal.Inc()
		observeWrite(start, nil)
	}()

	if r.closed.Load() {
		return ErrDatabaseClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(records) == 0 {
		return nil
	}

	batch := r.db.NewBatch()
	defer batch.Close()

	for _, m := range records {
		if err := m.Validate(); err != nil {
			return fmt.Errorf("create_many: validate %s: %w", m.ID, err)
		}

		value, err := encodeMetadata(m)
		if err != nil {
			return fmt.Errorf("create_many: encode %s: %w", m.ID, err)
		}

		pk := encodePrimaryKey(m.ID)
		sk := encodeSecondaryKey(m.Bucket, m.Key)

		if err := batch.Set(pk, value, nil); err != nil {
			return fmt.Errorf("create_many: set primary %s: %w", m.ID, err)
		}
		if err := batch.Set(sk, pk, nil); err != nil {
			return fmt.Errorf("create_many: set secondary %s: %w", m.ID, err)
		}
	}

	if err := batch.Commit(r.writeOpts); err != nil {
		return fmt.Errorf("create_many: commit: %w", err)
	}

	return nil
}

// DeleteMany atomically removes multiple metadata records by ID.
// Non-existent IDs are silently skipped.
func (r *PebbleMetadataRepository) DeleteMany(ctx context.Context, ids []string) error {
	start := time.Now()
	defer func() {
		metadataBatchOpsTotal.Inc()
		observeDelete(start, nil)
	}()

	if r.closed.Load() {
		return ErrDatabaseClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if len(ids) == 0 {
		return nil
	}

	batch := r.db.NewBatch()
	defer batch.Close()

	for _, id := range ids {
		pk := encodePrimaryKey(id)

		// Read to get bucket/key for secondary cleanup.
		value, closer, err := r.db.Get(pk)
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				continue // Idempotent.
			}
			return fmt.Errorf("delete_many: read %s: %w", id, err)
		}

		m, decErr := decodeMetadata(value)
		closer.Close()

		if decErr == nil {
			sk := encodeSecondaryKey(m.Bucket, m.Key)
			if err := batch.Delete(sk, nil); err != nil {
				return fmt.Errorf("delete_many: secondary %s: %w", id, err)
			}
		}

		if err := batch.Delete(pk, nil); err != nil {
			return fmt.Errorf("delete_many: primary %s: %w", id, err)
		}
	}

	if err := batch.Commit(r.writeOpts); err != nil {
		return fmt.Errorf("delete_many: commit: %w", err)
	}

	return nil
}

// Close shuts down the repository, flushing and closing the Pebble database.
// Safe to call multiple times; only the first call has effect.
func (r *PebbleMetadataRepository) Close() error {
	var err error
	r.once.Do(func() {
		r.closed.Store(true)
		r.logger.Info("closing pebble metadata store")
		err = r.db.Close()
		if err != nil {
			r.logger.Error("error closing pebble", zap.Error(err))
		} else {
			r.logger.Info("pebble metadata store closed")
		}
	})
	return err
}

// emptyIterator is returned when a List query has no results.
type emptyIterator struct{}

func (e *emptyIterator) Next() bool                          { return false }
func (e *emptyIterator) Item() (*metadata.ObjectMetadata, error) { return nil, metadata.ErrMetadataNotFound }
func (e *emptyIterator) Cursor() string                      { return "" }
func (e *emptyIterator) Err() error                          { return nil }
func (e *emptyIterator) Close() error                        { return nil }

// incrementBytes returns the next lexicographic byte sequence after b.
// Used to create exclusive upper bounds for prefix scans.
func incrementBytes(b []byte) []byte {
	result := make([]byte, len(b))
	copy(result, b)
	for i := len(result) - 1; i >= 0; i-- {
		if result[i] < 0xFF {
			result[i]++
			return result[:i+1]
		}
	}
	// All 0xFF; extend.
	return append(result, 0x00)
}