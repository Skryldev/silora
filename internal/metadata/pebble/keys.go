package pebble

import (
	"encoding/binary"
	"fmt"
)

// Key schema design:
//
// Primary record (by object ID):
//   [0x01][objectID bytes]
//
// Secondary index (by bucket + key):
//   [0x02][uint16 len(bucket)][bucket bytes][uint32 len(key)][key bytes]
//
// Bucket prefix (for listing all objects in a bucket):
//   [0x02][uint16 len(bucket)][bucket bytes][0xFF]
//
// Design rationale:
// - Binary prefixes (0x01, 0x02) provide O(1) record type discrimination.
// - Object IDs are UUIDs (16 bytes), stored as raw bytes for compactness.
// - Bucket and key use length-prefixed encoding to avoid separator collisions.
// - No escaping is needed; length prefixes make parsing unambiguous.
// - Lexicographic ordering within a bucket is preserved for key bytes,
//   enabling efficient prefix scans.
// - The 0xFF suffix on bucket prefix ensures the prefix scan captures all
//   keys within that bucket without bleeding into the next bucket.

const (
	keyPrefixPrimary   byte = 0x01
	keyPrefixSecondary byte = 0x02
	keyPrefixMax       byte = 0xFF
)

// Estimated key sizes for preallocation.
const (
	primary_key_size   = 1 + 36 // prefix + UUID string max
	secondary_key_base = 1 + 2 + 4 + 1 // prefix + bucket len + key len + separator
)

// encodePrimaryKey constructs the Pebble key for a primary object record.
// Format: [0x01][objectID]
// The objectID is stored as-is (expected to be a UUID string, 36 bytes).
func encodePrimaryKey(id string) []byte {
	buf := make([]byte, 0, 1+len(id))
	buf = append(buf, keyPrefixPrimary)
	buf = append(buf, id...)
	return buf
}

// encodeSecondaryKey constructs the Pebble key for the bucket+key index.
// Format: [0x02][uint16(len(bucket))][bucket][uint32(len(key))][key]
func encodeSecondaryKey(bucket, key string) []byte {
	size := 1 + 2 + len(bucket) + 4 + len(key)
	buf := make([]byte, 0, size)
	buf = append(buf, keyPrefixSecondary)
	buf = appendUint16(buf, uint16(len(bucket)))
	buf = append(buf, bucket...)
	buf = appendUint32(buf, uint32(len(key)))
	buf = append(buf, key...)
	return buf
}

// encodeBucketPrefix constructs the prefix for scanning all keys in a bucket.
// Format: [0x02][uint16(len(bucket))][bucket][0xFF]
func encodeBucketPrefix(bucket string) []byte {
	buf := make([]byte, 0, 1+2+len(bucket)+1)
	buf = append(buf, keyPrefixSecondary)
	buf = appendUint16(buf, uint16(len(bucket)))
	buf = append(buf, bucket...)
	buf = append(buf, keyPrefixMax)
	return buf
}

// encodeBucketKeyPrefix constructs the prefix for scanning keys with a given
// key prefix within a bucket.
// Format: [0x02][uint16(len(bucket))][bucket][uint32(len(keyPrefix))][keyPrefix]
func encodeBucketKeyPrefix(bucket, keyPrefix string) []byte {
	size := 1 + 2 + len(bucket) + 4 + len(keyPrefix)
	buf := make([]byte, 0, size)
	buf = append(buf, keyPrefixSecondary)
	buf = appendUint16(buf, uint16(len(bucket)))
	buf = append(buf, bucket...)
	buf = appendUint32(buf, uint32(len(keyPrefix)))
	buf = append(buf, keyPrefix...)
	return buf
}

// decodeSecondaryKey parses a secondary index key back into bucket and key.
// Returns an error if the key is malformed.
func decodeSecondaryKey(data []byte) (bucket, key string, err error) {
	if len(data) < 3 { // prefix + at least uint16
		return "", "", fmt.Errorf("secondary key too short: %d bytes", len(data))
	}
	if data[0] != keyPrefixSecondary {
		return "", "", fmt.Errorf("invalid secondary key prefix: 0x%02x", data[0])
	}

	pos := 1
	if pos+2 > len(data) {
		return "", "", fmt.Errorf("secondary key truncated at bucket length")
	}
	bucketLen := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2

	if pos+bucketLen > len(data) {
		return "", "", fmt.Errorf("secondary key truncated at bucket data")
	}
	bucket = string(data[pos : pos+bucketLen])
	pos += bucketLen

	if pos+4 > len(data) {
		return "", "", fmt.Errorf("secondary key truncated at key length")
	}
	keyLen := int(binary.BigEndian.Uint32(data[pos:]))
	pos += 4

	if pos+keyLen > len(data) {
		return "", "", fmt.Errorf("secondary key truncated at key data")
	}
	key = string(data[pos : pos+keyLen])

	return bucket, key, nil
}

// isPrimaryKey returns true if the given Pebble key is a primary record key.
func isPrimaryKey(key []byte) bool {
	return len(key) > 0 && key[0] == keyPrefixPrimary
}

// isSecondaryKey returns true if the given Pebble key is a secondary index key.
func isSecondaryKey(key []byte) bool {
	return len(key) > 0 && key[0] == keyPrefixSecondary
}

// appendUint16 appends a big-endian uint16 to buf.
func appendUint16(buf []byte, v uint16) []byte {
	return append(buf, byte(v>>8), byte(v))
}

// appendUint32 appends a big-endian uint32 to buf.
func appendUint32(buf []byte, v uint32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

// appendUint64 appends a big-endian uint64 to buf.
func appendUint64(buf []byte, v uint64) []byte {
	return append(buf,
		byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v),
	)
}