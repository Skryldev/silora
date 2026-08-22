package pebble

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/Skryldev/silora/internal/metadata"
)

// Schema versioning:
// Version 1: Initial schema.
//   Layout: [version:1][id_len:2][id][bucket_len:2][bucket][key_len:4][key]
//           [size:8][content_type_len:2][content_type][etag_len:2][etag]
//           [checksum_len:2][checksum][obj_version:8][state:1]
//           [created_at:8][updated_at:8]
//           [meta_count:2]{[k_len:2][k][v_len:4][v]}*
//
// Future versions append fields or restructure. The decoder checks the version
// byte and dispatches to the appropriate decoder. Unknown future versions are
// rejected with ErrUnsupportedVersion.

const (
	currentSchemaVersion byte = 1
	minSupportedVersion  byte = 1
	maxSupportedVersion  byte = 1
)

// encodeMetadata serializes an ObjectMetadata into the versioned binary format.
func encodeMetadata(m *metadata.ObjectMetadata) ([]byte, error) {
	// Preallocate with estimated size.
	estSize := 1 + // version
		2 + len(m.ID) +
		2 + len(m.Bucket) +
		4 + len(m.Key) +
		8 + // size
		2 + len(m.ContentType) +
		2 + len(m.ETag) +
		2 + len(m.Checksum) +
		8 + // version
		1 + // state
		8 + 8 + // timestamps
		2 // metadata count

	for k, v := range m.Metadata {
		estSize += 2 + len(k) + 4 + len(v)
	}

	buf := make([]byte, 0, estSize)

	// Schema version
	buf = append(buf, currentSchemaVersion)

	// ID
	buf = appendString16(buf, m.ID)

	// Bucket
	buf = appendString16(buf, m.Bucket)

	// Key
	buf = appendString32(buf, m.Key)

	// Size
	buf = appendInt64(buf, m.Size)

	// ContentType
	buf = appendString16(buf, m.ContentType)

	// ETag
	buf = appendString16(buf, m.ETag)

	// Checksum
	buf = appendString16(buf, m.Checksum)

	// Object version
	buf = appendUint64(buf, m.Version)

	// State
	buf = append(buf, byte(m.State))

	// CreatedAt (unix nanos)
	buf = appendInt64(buf, m.CreatedAt.UnixNano())

	// UpdatedAt (unix nanos)
	buf = appendInt64(buf, m.UpdatedAt.UnixNano())

	// Custom metadata
	if len(m.Metadata) > metadata.MaxMetadataEntries {
		return nil, metadata.ErrMetadataTooLarge
	}
	buf = appendUint16(buf, uint16(len(m.Metadata)))
	for k, v := range m.Metadata {
		if len(v) > metadata.MaxMetadataValueSize {
			return nil, metadata.ErrMetadataTooLarge
		}
		buf = appendString16(buf, k)
		buf = appendString32(buf, v)
	}

	return buf, nil
}

// decodeMetadata deserializes a versioned binary record into ObjectMetadata.
// It copies all data out of the input slice; the input may be Pebble-owned memory.
func decodeMetadata(data []byte) (*metadata.ObjectMetadata, error) {
	if len(data) < 1 {
		return nil, fmt.Errorf("%w: empty record", metadata.ErrMetadataCorrupted)
	}

	version := data[0]
	if version < minSupportedVersion || version > maxSupportedVersion {
		return nil, fmt.Errorf("%w: version %d", metadata.ErrUnsupportedVersion, version)
	}

	switch version {
	case 1:
		return decodeMetadataV1(data[1:])
	default:
		return nil, fmt.Errorf("%w: version %d", metadata.ErrUnsupportedVersion, version)
	}
}

func decodeMetadataV1(data []byte) (*metadata.ObjectMetadata, error) {
	m := &metadata.ObjectMetadata{}
	pos := 0

	// ID
	id, n, err := readString16(data, pos)
	if err != nil {
		return nil, wrapCorrupt("id", err)
	}
	m.ID = id
	pos += n

	// Bucket
	bucket, n, err := readString16(data, pos)
	if err != nil {
		return nil, wrapCorrupt("bucket", err)
	}
	m.Bucket = bucket
	pos += n

	// Key
	key, n, err := readString32(data, pos)
	if err != nil {
		return nil, wrapCorrupt("key", err)
	}
	m.Key = key
	pos += n

	// Size
	if pos+8 > len(data) {
		return nil, wrapCorrupt("size", fmt.Errorf("truncated"))
	}
	m.Size = int64(binary.BigEndian.Uint64(data[pos:]))
	pos += 8

	// ContentType
	ct, n, err := readString16(data, pos)
	if err != nil {
		return nil, wrapCorrupt("content_type", err)
	}
	m.ContentType = ct
	pos += n

	// ETag
	etag, n, err := readString16(data, pos)
	if err != nil {
		return nil, wrapCorrupt("etag", err)
	}
	m.ETag = etag
	pos += n

	// Checksum
	cs, n, err := readString16(data, pos)
	if err != nil {
		return nil, wrapCorrupt("checksum", err)
	}
	m.Checksum = cs
	pos += n

	// Version
	if pos+8 > len(data) {
		return nil, wrapCorrupt("version", fmt.Errorf("truncated"))
	}
	m.Version = binary.BigEndian.Uint64(data[pos:])
	pos += 8

	// State
	if pos+1 > len(data) {
		return nil, wrapCorrupt("state", fmt.Errorf("truncated"))
	}
	m.State = metadata.ObjectState(data[pos])
	pos += 1

	// CreatedAt
	if pos+8 > len(data) {
		return nil, wrapCorrupt("created_at", fmt.Errorf("truncated"))
	}
	m.CreatedAt = time.Unix(0, int64(binary.BigEndian.Uint64(data[pos:])))
	pos += 8

	// UpdatedAt
	if pos+8 > len(data) {
		return nil, wrapCorrupt("updated_at", fmt.Errorf("truncated"))
	}
	m.UpdatedAt = time.Unix(0, int64(binary.BigEndian.Uint64(data[pos:])))
	pos += 8

	// Custom metadata
	if pos+2 > len(data) {
		return nil, wrapCorrupt("meta_count", fmt.Errorf("truncated"))
	}
	metaCount := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2

	if metaCount > 0 {
		m.Metadata = make(map[string]string, metaCount)
		for i := 0; i < metaCount; i++ {
			k, n, err := readString16(data, pos)
			if err != nil {
				return nil, wrapCorrupt(fmt.Sprintf("meta_key[%d]", i), err)
			}
			pos += n

			v, n, err := readString32(data, pos)
			if err != nil {
				return nil, wrapCorrupt(fmt.Sprintf("meta_val[%d]", i), err)
			}
			pos += n

			m.Metadata[k] = v
		}
	}

	return m, nil
}

// Helper encoding functions.

func appendString16(buf []byte, s string) []byte {
	buf = appendUint16(buf, uint16(len(s)))
	buf = append(buf, s...)
	return buf
}

func appendString32(buf []byte, s string) []byte {
	buf = appendUint32(buf, uint32(len(s)))
	buf = append(buf, s...)
	return buf
}

func appendInt64(buf []byte, v int64) []byte {
	return appendUint64(buf, uint64(v))
}

// Helper decoding functions. All return copies (string() creates a new allocation).

func readString16(data []byte, pos int) (string, int, error) {
	if pos+2 > len(data) {
		return "", 0, fmt.Errorf("truncated at pos %d", pos)
	}
	length := int(binary.BigEndian.Uint16(data[pos:]))
	pos += 2
	if pos+length > len(data) {
		return "", 0, fmt.Errorf("string of len %d exceeds data at pos %d", length, pos)
	}
	s := string(data[pos : pos+length]) // deliberate copy
	return s, 2 + length, nil
}

func readString32(data []byte, pos int) (string, int, error) {
	if pos+4 > len(data) {
		return "", 0, fmt.Errorf("truncated at pos %d", pos)
	}
	length := int(binary.BigEndian.Uint32(data[pos:]))
	pos += 4
	if pos+length > len(data) {
		return "", 0, fmt.Errorf("string of len %d exceeds data at pos %d", length, pos)
	}
	s := string(data[pos : pos+length]) // deliberate copy
	return s, 4 + length, nil
}

func wrapCorrupt(field string, err error) error {
	return fmt.Errorf("%w: field %s: %v", metadata.ErrMetadataCorrupted, field, err)
}