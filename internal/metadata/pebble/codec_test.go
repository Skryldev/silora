package pebble

import (
	"testing"
	"time"

	"github.com/Skryldev/silora/internal/metadata"
)

func TestEncodeDecodeMetadata(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Microsecond) // Truncate to avoid nano precision issues in some DBs

	original := &metadata.ObjectMetadata{
		ID:          "550e8400-e29b-41d4-a716-446655440000",
		Bucket:      "test-bucket",
		Key:         "path/to/object.txt",
		Size:        1024 * 1024 * 5,
		ContentType: "text/plain",
		ETag:        "abc123def456",
		Checksum:    "sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
		Version:     42,
		State:       metadata.StateAvailable,
		CreatedAt:   now,
		UpdatedAt:   now.Add(time.Hour),
		Metadata: map[string]string{
			"author":      "alice",
			"department":  "engineering",
			"custom_flag": "true",
		},
	}

	encoded, err := encodeMetadata(original)
	if err != nil {
		t.Fatalf("encode failed: %v", err)
	}

	decoded, err := decodeMetadata(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID mismatch: got %q, want %q", decoded.ID, original.ID)
	}
	if decoded.Bucket != original.Bucket {
		t.Errorf("Bucket mismatch")
	}
	if decoded.Key != original.Key {
		t.Errorf("Key mismatch")
	}
	if decoded.Size != original.Size {
		t.Errorf("Size mismatch: got %d, want %d", decoded.Size, original.Size)
	}
	if decoded.ContentType != original.ContentType {
		t.Errorf("ContentType mismatch")
	}
	if decoded.ETag != original.ETag {
		t.Errorf("ETag mismatch")
	}
	if decoded.Checksum != original.Checksum {
		t.Errorf("Checksum mismatch")
	}
	if decoded.Version != original.Version {
		t.Errorf("Version mismatch: got %d, want %d", decoded.Version, original.Version)
	}
	if decoded.State != original.State {
		t.Errorf("State mismatch: got %v, want %v", decoded.State, original.State)
	}
	if !decoded.CreatedAt.Equal(original.CreatedAt) {
		t.Errorf("CreatedAt mismatch: got %v, want %v", decoded.CreatedAt, original.CreatedAt)
	}
	if !decoded.UpdatedAt.Equal(original.UpdatedAt) {
		t.Errorf("UpdatedAt mismatch")
	}
	if len(decoded.Metadata) != len(original.Metadata) {
		t.Fatalf("Metadata length mismatch: got %d, want %d", len(decoded.Metadata), len(original.Metadata))
	}
	for k, v := range original.Metadata {
		if decoded.Metadata[k] != v {
			t.Errorf("Metadata[%q] mismatch: got %q, want %q", k, decoded.Metadata[k], v)
		}
	}
}

func TestDecodeMetadata_Corruption(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte{}},
		{"unsupported_version", []byte{99}},
		{"truncated_v1", []byte{1, 0x00, 0x02, 'i', 'd'}}, // truncated before bucket
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeMetadata(tt.data)
			if err == nil {
				t.Fatal("expected error for corrupted data, got nil")
			}
		})
	}
}

func TestEncodeMetadata_TooLarge(t *testing.T) {
	m := &metadata.ObjectMetadata{
		ID:       "id",
		Bucket:   "b",
		Key:      "k",
		Metadata: make(map[string]string),
	}

	// Exceed MaxMetadataEntries
	for i := 0; i < metadata.MaxMetadataEntries+1; i++ {
		m.Metadata[string(rune(i))] = "v"
	}

	_, err := encodeMetadata(m)
	if err != metadata.ErrMetadataTooLarge {
		t.Fatalf("expected ErrMetadataTooLarge, got %v", err)
	}

	// Exceed MaxMetadataValueSize
	m.Metadata = map[string]string{
		"large": string(make([]byte, metadata.MaxMetadataValueSize+1)),
	}
	_, err = encodeMetadata(m)
	if err != metadata.ErrMetadataTooLarge {
		t.Fatalf("expected ErrMetadataTooLarge for value size, got %v", err)
	}
}