package pebble

import (
	"encoding/base64"
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/Skryldev/silora/internal/metadata"
)

type pebbleMetadataIterator struct {
	db       *pebble.DB // Added to perform primary key lookups
	iter     *pebble.Iterator
	mu       sync.Mutex
	closed   bool
	started  bool // Tracks if we've processed the initial positioning
	current  *metadata.ObjectMetadata
	err      error
	limit    int
	count    int
	lastKey  []byte
}

func newPebbleMetadataIterator(db *pebble.DB, iter *pebble.Iterator, limit int) *pebbleMetadataIterator {
	return &pebbleMetadataIterator{
		db:    db,
		iter:  iter,
		limit: limit,
	}
}

func (it *pebbleMetadataIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.err != nil {
		return false
	}

	if it.limit > 0 && it.count >= it.limit {
		return false
	}

	var valid bool
	if !it.started {
		// The repository already positioned the iterator using First().
		// We just check if it's valid.
		valid = it.iter.Valid()
		it.started = true
	} else {
		valid = it.iter.Next()
	}

	if !valid {
		it.err = it.iter.Error()
		return false
	}

	// The iterator is scanning secondary keys.
	// The value of a secondary key is the primary key.
	pk := it.iter.Value()
	
	// Copy the primary key because it.iter.Value() is only valid until the next iterator operation.
	pkCopy := make([]byte, len(pk))
	copy(pkCopy, pk)

	// Fetch the actual metadata record using the primary key.
	primaryValue, closer, err := it.db.Get(pkCopy)
	if err != nil {
		it.err = fmt.Errorf("iterator: primary lookup failed: %w", err)
		return false
	}

	m, err := decodeMetadata(primaryValue)
	closer.Close() // Release Pebble-owned memory

	if err != nil {
		it.err = err
		return false
	}

	it.current = m
	it.count++

	// Store the secondary key for cursor generation.
	key := it.iter.Key()
	it.lastKey = make([]byte, len(key))
	copy(it.lastKey, key)

	return true
}

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

func (it *pebbleMetadataIterator) Cursor() string {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.lastKey == nil {
		return ""
	}

	cursorData := make([]byte, 0, 1+len(it.lastKey))
	cursorData = append(cursorData, 0x01) // cursor format version
	cursorData = append(cursorData, it.lastKey...)
	return base64.RawURLEncoding.EncodeToString(cursorData)
}

func (it *pebbleMetadataIterator) Err() error {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.err
}

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

	version := data[0]
	if version != 0x01 {
		return nil, &metadata.MetadataError{
			Op:  "decode_cursor",
			Err: metadata.ErrUnsupportedVersion,
		}
	}

	key := make([]byte, len(data)-1)
	copy(key, data[1:])
	return key, nil
}