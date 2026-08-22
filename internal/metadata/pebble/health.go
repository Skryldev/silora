package pebble

import (
	"context"
)

// healthCheckKey is a well-known key used for health probes.
var healthCheckKey = []byte{0xFF, 0xFF, 'h', 'e', 'a', 'l', 't', 'h'}
var healthCheckValue = []byte{0x01}

// HealthCheck verifies the Pebble database is operational by performing
// a lightweight write and read.
func (r *PebbleMetadataRepository) HealthCheck(ctx context.Context) error {
	if r.closed.Load() {
		return ErrDatabaseClosed
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Use r.writeOpts instead of nil
	if err := r.db.Set(healthCheckKey, healthCheckValue, r.writeOpts); err != nil {
		return err
	}

	val, closer, err := r.db.Get(healthCheckKey)
	if err != nil {
		return err
	}
	closer.Close()
	_ = val

	return nil
}

// Flush forces a memtable flush to disk.
func (r *PebbleMetadataRepository) Flush() error {
	if r.closed.Load() {
		return ErrDatabaseClosed
	}
	return r.db.Flush()
}

// Compact triggers a manual compaction over the entire key range.
// This is an expensive operation; use sparingly.
func (r *PebbleMetadataRepository) Compact() error {
	if r.closed.Load() {
		return ErrDatabaseClosed
	}
	start := []byte{0x00}
	end := []byte{0xFF, 0xFF, 0xFF, 0xFF}
	return r.db.Compact(start, end, false)
}