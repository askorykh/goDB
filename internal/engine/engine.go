package engine

import (
	"fmt"
	"goDB/internal/sql"
	"goDB/internal/storage"
)

// DBEngine coordinates database operations on top of a storage engine.
// Later it will have references to catalog metadata, query planner, and other
// subsystems, but for now it focuses on basic table lifecycle and row access.
type DBEngine struct {
	started bool
	store   storage.Engine
}

// New creates a new DBEngine instance backed by the provided storage engine.
func New(store storage.Engine) *DBEngine {
	return &DBEngine{
		started: false,
		store:   store,
	}
}

// Start runs initialization steps for the engine.
// Future versions will open storage, load metadata, and possibly run recovery.
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
// The helper wraps begin/commit logic so callers do not need to manage
// transactions for simple inserts, and it rolls back the transaction on
// failure to keep the table state consistent.
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
// It executes inside a read-only transaction so future engines can enforce
// isolation without changing the call site.
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
