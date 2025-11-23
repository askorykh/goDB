package memstore

import (
	"fmt"
	"goDB/internal/sql"
	"goDB/internal/storage"
	"sort"
	"sync"
)

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

func (e *memEngine) ListTables() ([]string, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	names := make([]string, 0, len(e.tables))
	for name := range e.tables {
		names = append(names, name)
	}

	sort.Strings(names)
	return names, nil
}

func (e *memEngine) TableSchema(name string) ([]sql.Column, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	t, ok := e.tables[name]
	if !ok {
		return nil, fmt.Errorf("table %s does not exist", name)
	}

	cols := make([]sql.Column, len(t.cols))
	copy(cols, t.cols)
	return cols, nil
}

// memTx represents a transaction on top of memEngine.
type memTx struct {
	eng      *memEngine
	readOnly bool
	tables   map[string]*table
}

func cloneTable(t *table) *table {
	colsCopy := make([]sql.Column, len(t.cols))
	copy(colsCopy, t.cols)

	rowsCopy := make([]sql.Row, len(t.rows))
	for i, r := range t.rows {
		rowCopy := make(sql.Row, len(r))
		copy(rowCopy, r)
		rowsCopy[i] = rowCopy
	}

	return &table{
		name: t.name,
		cols: colsCopy,
		rows: rowsCopy,
	}
}

func (tx *memTx) ReplaceAll(tableName string, rows []sql.Row) error {
	if tx.readOnly {
		return fmt.Errorf("cannot replace in a read-only transaction")
	}

	t, ok := tx.tables[tableName]
	if !ok {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	// basic type/length validation for safety
	for _, r := range rows {
		if len(r) != len(t.cols) {
			return fmt.Errorf("column count mismatch in ReplaceAll: expected %d, got %d", len(t.cols), len(r))
		}
		for i, col := range t.cols {
			if r[i].Type != col.Type {
				return fmt.Errorf("type mismatch in ReplaceAll for column %q: expected %v, got %v",
					col.Name, col.Type, r[i].Type)
			}
		}
	}

	// store a deep copy to avoid external modification
	newRows := make([]sql.Row, len(rows))
	for i, r := range rows {
		rowCopy := make(sql.Row, len(r))
		copy(rowCopy, r)
		newRows[i] = rowCopy
	}

	t.rows = newRows
	return nil
}

func (tx *memTx) Scan(tableName string) (col []string, rows []sql.Row, err error) {
	t, ok := tx.tables[tableName]
	if !ok {
		return nil, nil, fmt.Errorf("table %s does not exist", tableName)
	}

	// Extract column names from the column metadata.
	colNames := make([]string, len(t.cols))
	for i, c := range t.cols {
		colNames[i] = c.Name
	}

	// Return a deep copy to prevent callers from mutating stored data.
	rowsCopy := make([]sql.Row, len(t.rows))
	for i, r := range t.rows {
		rowCopy := make(sql.Row, len(r))
		copy(rowCopy, r)
		rowsCopy[i] = rowCopy
	}

	return colNames, rowsCopy, nil
}

// Begin starts a new transaction.
func (e *memEngine) Begin(readOnly bool) (storage.Tx, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	tablesCopy := make(map[string]*table, len(e.tables))
	for name, t := range e.tables {
		tablesCopy[name] = cloneTable(t)
	}

	return &memTx{
		eng:      e,
		readOnly: readOnly,
		tables:   tablesCopy,
	}, nil
}

// Commit finishes a transaction.
func (e *memEngine) Commit(tx storage.Tx) error {
	m, ok := tx.(*memTx)
	if !ok {
		return fmt.Errorf("invalid transaction type")
	}
	if m.readOnly {
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	e.tables = m.tables
	return nil
}

// Rollback aborts a transaction.
// For this simple in-memory implementation, it's a no-op.
func (e *memEngine) Rollback(tx storage.Tx) error {
	return nil
}

// Insert adds a row into a table inside this transaction.
func (tx *memTx) Insert(tableName string, row sql.Row) error {
	if tx.readOnly {
		return fmt.Errorf("cannot insert in a read-only transaction")
	}

	t, ok := tx.tables[tableName]
	if !ok {
		return fmt.Errorf("table %s does not exist", tableName)
	}

	if len(row) != len(t.cols) {
		return fmt.Errorf("column count mismatch: expected %d, got %d", len(t.cols), len(row))
	}

	// Type check each value against the column definition.
	for i, col := range t.cols {
		if row[i].Type != col.Type && row[i].Type != sql.TypeNull {
			return fmt.Errorf("type mismatch for column %q: expected %v, got %v",
				col.Name, col.Type, row[i].Type)
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
