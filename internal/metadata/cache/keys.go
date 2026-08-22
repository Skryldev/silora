package cache

import "strings"

const (
	prefixID        = "id:"
	prefixBucketKey = "bk:"
)

// KeyByID generates a deterministic cache key for primary lookups.
func KeyByID(id string) string {
	var b strings.Builder
	b.Grow(len(prefixID) + len(id))
	b.WriteString(prefixID)
	b.WriteString(id)
	return b.String()
}

// KeyByBucketAndKey generates a deterministic cache key for secondary lookups.
// Uses a null byte separator to prevent collision between bucket/key boundaries.
func KeyByBucketAndKey(bucket, key string) string {
	var b strings.Builder
	b.Grow(len(prefixBucketKey) + len(bucket) + 1 + len(key))
	b.WriteString(prefixBucketKey)
	b.WriteString(bucket)
	b.WriteByte(0) 
	b.WriteString(key)
	return b.String()
}