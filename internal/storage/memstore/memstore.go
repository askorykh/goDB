package memstore

import (
	"fmt"
	"goDB/internal/sql"
	"goDB/internal/storage"
	"sync"
)

// table keeps the column schema and stored rows for a single in-memory table.
// The enclosing memEngine mutex guards access to both fields.
type table struct {
	name string
	cols []sql.Column // column names
	rows []sql.Row    // stored rows
}

type memEngine struct {
	mu     sync.RWMutex
	tables map[string]*table
}

// New creates a new in-memory storage engine.
func New() storage.Engine {
	return &memEngine{
		tables: make(map[string]*table),
	}
}

// memTx represents a transaction on top of memEngine.
// It keeps a pointer to the parent engine so it can reuse the shared mutex and
// table map; write operations simply append to in-memory slices.
type memTx struct {
	eng      *memEngine
	readOnly bool
}

func (tx *memTx) Scan(tableName string) (col []string, rows []sql.Row, err error) {
	tx.eng.mu.RLock()
	defer tx.eng.mu.RUnlock()

	t, ok := tx.eng.tables[tableName]
	if !ok {
		return nil, nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Extract column names from the column metadata.
	colNames := make([]string, len(t.cols))
	for i, c := range t.cols {
		colNames[i] = c.Name
	}

	// We return the slice directly for now for simplicity. In a production
	// engine, this would likely copy data or expose an iterator to avoid
	// accidental mutations by callers.
	return colNames, t.rows, nil
}

// Begin starts a new transaction.
func (e *memEngine) Begin(readOnly bool) (storage.Tx, error) {
	return &memTx{
		eng:      e,
		readOnly: readOnly,
	}, nil
}

// Commit finishes a transaction.
// For this simple in-memory implementation, it's a no-op because data is
// already visible after writes.
func (e *memEngine) Commit(tx storage.Tx) error {
	return nil
}

// Rollback aborts a transaction.
// For this simple in-memory implementation, it's a no-op because writes are
// applied directly to the in-memory table slices.
func (e *memEngine) Rollback(tx storage.Tx) error {
	return nil
}

// Insert adds a row into a table inside this transaction.
// It performs basic length and type validation before appending to the table.
func (tx *memTx) Insert(tableName string, row sql.Row) error {
	if tx.readOnly {
		return fmt.Errorf("cannot insert in a read-only transaction")
	}

	tx.eng.mu.Lock()
	defer tx.eng.mu.Unlock()

	t, ok := tx.eng.tables[tableName]
	if !ok {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	if len(row) != len(t.cols) {
		return fmt.Errorf("column count mismatch: expected %d, got %d", len(t.cols), len(row))
	}

	// Type check each value against the column definition.
	for i, col := range t.cols {
		val := row[i]
		if val.Type != col.Type {
			return fmt.Errorf("type mismatch for column %q: expected %v, got %v", col.Name, col.Type, val.Type)
		}
	}

	t.rows = append(t.rows, row)
	return nil
}

// CreateTable is a helper to create a new table in memory.
// We'll call this from the engine or SQL layer later.
func (e *memEngine) CreateTable(name string, cols []sql.Column) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}

	e.tables[name] = &table{
		name: name,
		cols: cols,
		rows: make([]sql.Row, 0),
	}

	return nil
}
