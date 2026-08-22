package pebble

import (
	"bytes"
	"testing"
	"encoding/base64"
)

func TestCursorEncodeDecode(t *testing.T) {
	originalKey := encodeSecondaryKey("my-bucket", "some/object/key.txt")

	// Simulate iterator generating a cursor
	cursor := encodeCursor(originalKey)
	if cursor == "" {
		t.Fatal("cursor should not be empty")
	}

	// Decode the cursor
	decodedKey, err := decodeCursor(cursor)
	if err != nil {
		t.Fatalf("decode cursor failed: %v", err)
	}

	if !bytes.Equal(originalKey, decodedKey) {
		t.Fatalf("decoded key mismatch: got %x, want %x", decodedKey, originalKey)
	}
}

func TestDecodeCursor_Empty(t *testing.T) {
	key, err := decodeCursor("")
	if err != nil {
		t.Fatalf("empty cursor should not error: %v", err)
	}
	if key != nil {
		t.Fatal("empty cursor should return nil key")
	}
}

func TestDecodeCursor_Invalid(t *testing.T) {
	_, err := decodeCursor("not-valid-base64!!!")
	if err == nil {
		t.Fatal("expected error for invalid base64")
	}

	// Valid base64 but wrong version
	_, err = decodeCursor("AgEB") // version 2
	if err == nil {
		t.Fatal("expected error for unsupported cursor version")
	}
}

// Helper to simulate cursor generation from iterator
func encodeCursor(pebbleKey []byte) string {
	cursorData := make([]byte, 0, 1+len(pebbleKey))
	cursorData = append(cursorData, 0x01) // version 1
	cursorData = append(cursorData, pebbleKey...)

	return base64.RawURLEncoding.EncodeToString(cursorData)
}