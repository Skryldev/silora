package pebble

import (
	"bytes"
	"testing"
)

func TestEncodePrimaryKey(t *testing.T) {
	id := "550e8400-e29b-41d4-a716-446655440000"
	key := encodePrimaryKey(id)

	if len(key) == 0 {
		t.Fatal("primary key is empty")
	}
	if key[0] != keyPrefixPrimary {
		t.Fatalf("expected prefix 0x%02x, got 0x%02x", keyPrefixPrimary, key[0])
	}
	if string(key[1:]) != id {
		t.Fatalf("expected id %q, got %q", id, string(key[1:]))
	}
}

func TestEncodeDecodeSecondaryKey(t *testing.T) {
	tests := []struct {
		name   string
		bucket string
		key    string
	}{
		{"simple", "my-bucket", "file.txt"},
		{"with_slashes", "user-data", "docs/2024/report.pdf"},
		{"empty_key", "bucket", ""},
		{"long_strings", string(make([]byte, 200)), string(make([]byte, 500))},
		{"binary_looking", "b\x00\x01", "k\x00\xff"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := encodeSecondaryKey(tt.bucket, tt.key)

			if encoded[0] != keyPrefixSecondary {
				t.Fatalf("expected prefix 0x%02x, got 0x%02x", keyPrefixSecondary, encoded[0])
			}

			decBucket, decKey, err := decodeSecondaryKey(encoded)
			if err != nil {
				t.Fatalf("decode failed: %v", err)
			}

			if decBucket != tt.bucket {
				t.Fatalf("bucket mismatch: got %q, want %q", decBucket, tt.bucket)
			}
			if decKey != tt.key {
				t.Fatalf("key mismatch: got %q, want %q", decKey, tt.key)
			}
		})
	}
}

func TestDecodeSecondaryKey_Errors(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"too_short", []byte{keyPrefixSecondary, 0x00}},
		{"wrong_prefix", []byte{keyPrefixPrimary, 0x00, 0x01, 'a', 0x00, 0x00, 0x00, 0x01, 'b'}},
		{"truncated_bucket", []byte{keyPrefixSecondary, 0x00, 0x05, 'a', 'b'}},
		{"truncated_key_len", []byte{keyPrefixSecondary, 0x00, 0x01, 'a'}},
		{"truncated_key_data", []byte{keyPrefixSecondary, 0x00, 0x01, 'a', 0x00, 0x00, 0x00, 0x05, 'b'}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := decodeSecondaryKey(tt.data)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestBucketPrefixOrdering(t *testing.T) {
	// Verify that keys within a bucket sort correctly and don't bleed into the next bucket.
	b1 := "bucket-a"
	b2 := "bucket-b"

	k1 := encodeSecondaryKey(b1, "file1")
	k2 := encodeSecondaryKey(b1, "file2")
	k3 := encodeSecondaryKey(b2, "file1")

	if bytes.Compare(k1, k2) >= 0 {
		t.Error("k1 should be less than k2")
	}
	if bytes.Compare(k2, k3) >= 0 {
		t.Error("k2 should be less than k3")
	}

	prefix := encodeBucketPrefix(b1)
	// The prefix should be greater than all keys in bucket-a
	if bytes.Compare(prefix, k2) <= 0 {
		t.Error("bucket prefix should be greater than all keys in the bucket")
	}
	// The prefix should be less than keys in bucket-b
	if bytes.Compare(prefix, k3) >= 0 {
		t.Error("bucket prefix should be less than keys in the next bucket")
	}
}