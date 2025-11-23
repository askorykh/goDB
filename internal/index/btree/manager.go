package btree

import (
	"path/filepath"
	"sync"
)

// Manager manages B-Tree indexes in a directory (usually the db dir).
type Manager struct {
	dir  string
	mu   sync.Mutex
	open map[string]Index // key: indexFileName or "table.column"
}

// NewManager creates a new index manager rooted at dir.
func NewManager(dir string) *Manager {
	return &Manager{
		dir:  dir,
		open: make(map[string]Index),
	}
}

// indexFileName is a simple convention: table_column.idx
func indexFileName(table, col string) string {
	return table + "_" + col + ".idx"
}

// key for open map
func indexKey(table, col string) string {
	return table + "." + col
}

// OpenOrCreateIndex returns an Index for (table, col), creating the B-Tree
// file if needed.
func (m *Manager) OpenOrCreateIndex(table, col string) (Index, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	k := indexKey(table, col)
	if idx, ok := m.open[k]; ok {
		return idx, nil
	}

	fileName := indexFileName(table, col)
	path := filepath.Join(m.dir, fileName)

	// We'll implement OpenFileIndex in the next step (B-Tree on disk).
	idx, err := OpenFileIndex(path, Meta{
		TableName: table,
		Column:    col,
	})
	if err != nil {
		return nil, err
	}

	m.open[k] = idx
	return idx, nil
}

// CloseAll closes all open indexes.
func (m *Manager) CloseAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for k, idx := range m.open {
		if err := idx.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(m.open, k)
	}
	return firstErr
}
