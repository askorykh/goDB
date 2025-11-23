package btree

import "fmt"

// For now we only support int64 keys.
type Key = int64

// Meta carries basic information about an index.
type Meta struct {
	TableName string // e.g. "users"
	Column    string // e.g. "id"
}

// Index describes the operations a B-Tree index supports.
type Index interface {
	// Insert adds a mapping key -> rid.
	Insert(key Key, rid RID) error

	// Delete removes a specific mapping key -> rid.
	// If rid doesn't exist for that key, it's a no-op.
	Delete(key Key, rid RID) error

	// DeleteKey removes all RIDs for a given key (optional, but handy).
	DeleteKey(key Key) error

	// Search returns all RIDs for a key.
	Search(key Key) ([]RID, error)

	// Close flushes and closes the index file.
	Close() error
}

// ErrNotFound is returned when a key is not present in the index.
// (Search may just return empty slice + nil instead; we keep this for flexibility.)
var ErrNotFound = fmt.Errorf("btree: key not found")
