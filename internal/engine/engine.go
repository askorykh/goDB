package engine

import (
	"fmt"
	"goDB/internal/sql"
	"goDB/internal/storage"
)

// DBEngine is the main database engine struct.
// Later it will have references to storage, catalog, transaction manager, etc.
type DBEngine struct {
	started bool
	store   storage.Engine
}

// New creates a new DBEngine instance.
// For now, it just returns an empty engine.
func New(store storage.Engine) *DBEngine {
	return &DBEngine{
		started: false,
		store:   store,
	}
}

// Start runs initialization steps for the engine.
// Later this will open storage, load metadata, etc.
func (e *DBEngine) Start() error {
	if e.started {
		return fmt.Errorf("engine already started")
	}
	e.started = true
	return nil
}

// CreateTable creates a new table in the underlying storage engine.
func (e *DBEngine) CreateTable(name string, cols []sql.Column) error {
	if !e.started {
		return fmt.Errorf("engine not started")
	}
	return e.store.CreateTable(name, cols)
}

// InsertRow inserts a single row into the given table using a transaction.
func (e *DBEngine) InsertRow(tableName string, row sql.Row) error {
	if !e.started {
		return fmt.Errorf("engine not started")
	}

	// Start a read-write transaction.
	tx, err := e.store.Begin(false /* readOnly */)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	// Try to insert the row.
	if err := tx.Insert(tableName, row); err != nil {
		_ = e.store.Rollback(tx)
		return fmt.Errorf("insert: %w", err)
	}

	// Commit the transaction.
	if err := e.store.Commit(tx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// SelectAll returns all rows from the given table.
func (e *DBEngine) SelectAll(tableName string) ([]string, []sql.Row, error) {
	if !e.started {
		return nil, nil, fmt.Errorf("engine not started")
	}

	// Start a read-only transaction.
	tx, err := e.store.Begin(true /* readOnly */)
	if err != nil {
		return nil, nil, fmt.Errorf("begin tx: %w", err)
	}

	cols, rows, err := tx.Scan(tableName)
	if err != nil {
		_ = e.store.Rollback(tx)
		return nil, nil, fmt.Errorf("scan: %w", err)
	}

	if err := e.store.Commit(tx); err != nil {
		return nil, nil, fmt.Errorf("commit: %w", err)
	}

	return cols, rows, nil
}

// Execute takes a parsed SQL Statement and executes it using the engine.
// For now it only supports CREATE TABLE statements.
func (e *DBEngine) Execute(stmt sql.Statement) ([]string, []sql.Row, error) {
	if !e.started {
		return nil, nil, fmt.Errorf("engine not started")
	}

	switch s := stmt.(type) {
	case *sql.CreateTableStmt:
		err := e.CreateTable(s.TableName, s.Columns)
		return nil, nil, err

	case *sql.InsertStmt:
		err := e.InsertRow(s.TableName, s.Values)
		return nil, nil, err

	case *sql.SelectStmt:
		cols, rows, err := e.SelectAll(s.TableName)
		if err != nil {
			return nil, nil, err
		}

		// Apply WHERE filter if present.
		if s.Where != nil {
			rows = filterRowsWhere(cols, rows, s.Where)
		}

		return cols, rows, nil

	default:
		return nil, nil, fmt.Errorf("unsupported statement type %T", stmt)
	}
}

// filterRowsWhere filters rows according to a simple WHERE expression (column = literal).
func filterRowsWhere(cols []string, rows []sql.Row, where *sql.WhereExpr) []sql.Row {
	// Map column name -> index
	colIndex := make(map[string]int, len(cols))
	for i, name := range cols {
		colIndex[name] = i
	}

	idx, ok := colIndex[where.Column]
	if !ok {
		// Column not found: no rows match.
		return nil
	}

	var out []sql.Row
	for _, row := range rows {
		if idx < 0 || idx >= len(row) {
			continue
		}
		v := row[idx]
		if valuesEqual(v, where.Value) {
			out = append(out, row)
		}
	}

	return out
}

// valuesEqual compares two sql.Value for equality, considering their type.
func valuesEqual(a, b sql.Value) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case sql.TypeInt:
		return a.I64 == b.I64
	case sql.TypeFloat:
		return a.F64 == b.F64
	case sql.TypeString:
		return a.S == b.S
	case sql.TypeBool:
		return a.B == b.B
	default:
		return false
	}
}
