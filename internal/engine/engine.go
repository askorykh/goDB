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
		return cols, rows, err

	default:
		return nil, nil, fmt.Errorf("unsupported statement type %T", stmt)
	}
}
