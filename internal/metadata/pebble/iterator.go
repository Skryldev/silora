package pebble

import (
	"encoding/base64"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/Skryldev/silora/internal/metadata"
)

// pebbleMetadataIterator implements metadata.MetadataIterator using a Pebble iterator.
// It streams results without loading the entire dataset into memory.
type pebbleMetadataIterator struct {
	iter     *pebble.Iterator
	mu       sync.Mutex
	closed   bool
	current  *metadata.ObjectMetadata
	err      error
	limit    int
	count    int
	lastKey  []byte
}

// newPebbleMetadataIterator wraps a Pebble iterator with metadata decoding and pagination.
func newPebbleMetadataIterator(iter *pebble.Iterator, limit int) *pebbleMetadataIterator {
	return &pebbleMetadataIterator{
		iter:  iter,
		limit: limit,
	}
}

// Next advances the iterator to the next valid metadata record.
func (it *pebbleMetadataIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.err != nil {
		return false
	}

	if it.limit > 0 && it.count >= it.limit {
		return false
	}

	if !it.iter.Next() {
		it.err = it.iter.Error()
		return false
	}

	// Decode the value.
	value := it.iter.Value()
	m, err := decodeMetadata(value)
	if err != nil {
		it.err = err
		return false
	}

	it.current = m
	it.count++

	// Store the key for cursor generation.
	key := it.iter.Key()
	it.lastKey = make([]byte, len(key))
	copy(it.lastKey, key)

	return true
}

// Item returns the current metadata record.
func (it *pebbleMetadataIterator) Item() (*metadata.ObjectMetadata, error) {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil, metadata.ErrIteratorClosed
	}
	if it.current == nil {
		return nil, metadata.ErrMetadataNotFound
	}
	return it.current, nil
}

// Cursor returns an opaque pagination token for the current position.
// The cursor is a base64-encoded version of the last Pebble key,
// prefixed with a version byte for future format changes.
func (it *pebbleMetadataIterator) Cursor() string {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.lastKey == nil {
		return ""
	}

	// Cursor format: [version:1][pebble_key]
	cursorData := make([]byte, 0, 1+len(it.lastKey))
	cursorData = append(cursorData, 0x01) // cursor format version
	cursorData = append(cursorData, it.lastKey...)
	return base64.RawURLEncoding.EncodeToString(cursorData)
}

// Err returns any error encountered during iteration.
func (it *pebbleMetadataIterator) Err() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.err
}

// Close releases the underlying Pebble iterator.
func (it *pebbleMetadataIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil
	}
	it.closed = true
	it.current = nil
	it.lastKey = nil

	metadataIteratorOpsTotal.WithLabelValues("close").Inc()
	return it.iter.Close()
}

// decodeCursor parses an opaque cursor token back into a Pebble key.
// Returns nil if the cursor is empty (start from beginning).
func decodeCursor(cursor string) ([]byte, error) {
	if cursor == "" {
		return nil, nil
	}

	data, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, &metadata.MetadataError{
			Op:  "decode_cursor",
			Err: metadata.ErrMetadataInvalid,
		}
	}

	if len(data) < 1 {
		return nil, &metadata.MetadataError{
			Op:  "decode_cursor",
			Err: metadata.ErrMetadataInvalid,
		}
	}

	// Check cursor format version.
	version := data[0]
	if version != 0x01 {
		return nil, &metadata.MetadataError{
			Op:  "decode_cursor",
			Err: metadata.ErrUnsupportedVersion,
		}
	}

	// Return the Pebble key (copy to avoid aliasing).
	key := make([]byte, len(data)-1)
	copy(key, data[1:])
	return key, nil
}