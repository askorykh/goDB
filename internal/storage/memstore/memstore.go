package memstore

import (
	"fmt"
	"goDB/internal/index/btree"
	"goDB/internal/sql"
	"goDB/internal/storage"
	"sort"
	"strings"
	"sync"
)

type table struct {
	name string
	cols []sql.Column // column names
	rows []sql.Row    // stored rows
}

type index struct {
	name       string
	tableName  string
	columnName string
	btree      btree.Index
}

type memEngine struct {
	mu      sync.RWMutex
	tables  map[string]*table
	indexes map[string]*index
	idxMan  *btree.Manager
}

// New creates a new in-memory storage engine with the default data directory.
func New() storage.Engine {
	return NewWithDir("data")
}

// NewWithDir creates a new in-memory storage engine with the given data directory.
func NewWithDir(dir string) storage.Engine {
	return &memEngine{
		tables:  make(map[string]*table),
		indexes: make(map[string]*index),
		idxMan:  btree.NewManager(dir),
	}
}

func (e *memEngine) CreateIndex(indexName, tableName, columnName string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.indexes[indexName]; exists {
		return fmt.Errorf("index %q already exists", indexName)
	}

	tbl, ok := e.tables[tableName]
	if !ok {
		return fmt.Errorf("table %q not found", tableName)
	}

	colIdx := -1
	for i, col := range tbl.cols {
		if strings.EqualFold(col.Name, columnName) {
			colIdx = i
			break
		}
	}

	if colIdx == -1 {
		return fmt.Errorf("column %q not found in table %q", columnName, tableName)
	}

	if tbl.cols[colIdx].Type != sql.TypeInt {
		return fmt.Errorf("cannot create index on non-integer column %q", columnName)
	}

	bt, err := e.idxMan.OpenOrCreateIndex(tableName, columnName)
	if err != nil {
		return fmt.Errorf("could not create index: %w", err)
	}

	// Populate the index with existing data.
	for i, row := range tbl.rows {
		val := row[colIdx]
		if val.Type == sql.TypeNull {
			continue
		}
		rid := btree.RID{PageID: 0, SlotID: uint16(i)}
		if err := bt.Insert(val.I64, rid); err != nil {
			return fmt.Errorf("error building index: %w", err)
		}
	}

	e.indexes[indexName] = &index{
		name:       indexName,
		tableName:  tableName,
		columnName: columnName,
		btree:      bt,
	}

	return nil
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

func (tx *memTx) DeleteWhere(tableName string, pred storage.RowPredicate) error {
	if tx.readOnly {
		return fmt.Errorf("memstore: cannot delete in read-only transaction")
	}

	tbl, ok := tx.tables[tableName]
	if !ok {
		return fmt.Errorf("memstore: table %q does not exist", tableName)
	}

	var newRows []sql.Row
	for _, row := range tbl.rows {
		match, err := pred(row)
		if err != nil {
			return err
		}
		if !match {
			newRows = append(newRows, row)
		}
	}

	tbl.rows = newRows
	return nil
}

func (tx *memTx) UpdateWhere(tableName string, pred storage.RowPredicate, updater storage.RowUpdater) error {
	if tx.readOnly {
		return fmt.Errorf("memstore: cannot update in read-only transaction")
	}

	tbl, ok := tx.tables[tableName]
	if !ok {
		return fmt.Errorf("memstore: table %q does not exist", tableName)
	}

	for i, row := range tbl.rows {
		match, err := pred(row)
		if err != nil {
			return err
		}
		if !match {
			continue
		}

		newRow, err := updater(row)
		if err != nil {
			return err
		}

		tbl.rows[i] = newRow
	}

	return nil
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

	// Add the row to the table.
	newRowIndex := len(t.rows)
	t.rows = append(t.rows, row)

	// Update indexes.
	for _, idx := range tx.eng.indexes {
		if idx.tableName == tableName {
			colIdx := -1
			for i, col := range t.cols {
				if strings.EqualFold(col.Name, idx.columnName) {
					colIdx = i
					break
				}
			}
			if colIdx != -1 {
				val := row[colIdx]
				if val.Type != sql.TypeNull {
					rid := btree.RID{PageID: 0, SlotID: uint16(newRowIndex)}
					if err := idx.btree.Insert(val.I64, rid); err != nil {
						return fmt.Errorf("error updating index %q: %w", idx.name, err)
					}
				}
			}
		}
	}

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
