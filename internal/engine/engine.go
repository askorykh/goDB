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
	inTx    bool
	currTx  storage.Tx
}

// New creates a new DBEngine instance.
// For now, it just returns an empty engine.
func New(store storage.Engine) *DBEngine {
	return &DBEngine{
		started: false,
		store:   store,
		inTx:    false,
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

// ListTables returns the names of all tables in the storage engine.
func (e *DBEngine) ListTables() ([]string, error) {
	if !e.started {
		return nil, fmt.Errorf("engine not started")
	}

	return e.store.ListTables()
}

// TableSchema returns the column definitions for a table.
func (e *DBEngine) TableSchema(name string) ([]sql.Column, error) {
	if !e.started {
		return nil, fmt.Errorf("engine not started")
	}

	return e.store.TableSchema(name)
}

func (e *DBEngine) executeUpdate(stmt *sql.UpdateStmt) error {
	if stmt.Where == nil {
		return fmt.Errorf("UPDATE without WHERE is not supported yet")
	}

	if e.inTx {
		return e.executeUpdateInTx(e.currTx, stmt)
	}

	tx, err := e.store.Begin(false)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	if err := e.executeUpdateInTx(tx, stmt); err != nil {
		_ = e.store.Rollback(tx)
		return err
	}

	if err := e.store.Commit(tx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (e *DBEngine) executeUpdateInTx(tx storage.Tx, stmt *sql.UpdateStmt) error {
	cols, rows, err := tx.Scan(stmt.TableName)
	if err != nil {
		return fmt.Errorf("scan: %w", err)
	}

	newRows, _, err := applyUpdate(cols, rows, stmt.Where, stmt.Assignments)
	if err != nil {
		return err
	}

	if err := tx.ReplaceAll(stmt.TableName, newRows); err != nil {
		return fmt.Errorf("replaceAll: %w", err)
	}

	return nil
}

func (e *DBEngine) executeDelete(stmt *sql.DeleteStmt) error {
	if stmt.Where == nil {
		return fmt.Errorf("DELETE without WHERE is not supported yet")
	}

	if e.inTx {
		return e.executeDeleteInTx(e.currTx, stmt)
	}

	tx, err := e.store.Begin(false)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	if err := e.executeDeleteInTx(tx, stmt); err != nil {
		_ = e.store.Rollback(tx)
		return err
	}

	if err := e.store.Commit(tx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (e *DBEngine) executeDeleteInTx(tx storage.Tx, stmt *sql.DeleteStmt) error {
	cols, rows, err := tx.Scan(stmt.TableName)
	if err != nil {
		return fmt.Errorf("scan: %w", err)
	}

	newRows, _, err := applyDelete(cols, rows, stmt.Where)
	if err != nil {
		return err
	}
	if err := tx.ReplaceAll(stmt.TableName, newRows); err != nil {
		return fmt.Errorf("replaceAll: %w", err)
	}

	return nil
}

func (e *DBEngine) executeInsert(stmt *sql.InsertStmt) error {
	if e.inTx {
		return e.executeInsertInTx(e.currTx, stmt)
	}

	tx, err := e.store.Begin(false)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	if err := e.executeInsertInTx(tx, stmt); err != nil {
		_ = e.store.Rollback(tx)
		return err
	}

	if err := e.store.Commit(tx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return nil
}

// Uses an existing transaction (either currTx or a one-off).
func (e *DBEngine) executeInsertInTx(tx storage.Tx, stmt *sql.InsertStmt) error {
	// Use tx.Scan to get column names
	cols, _, err := tx.Scan(stmt.TableName)
	if err != nil {
		return fmt.Errorf("scan: %w", err)
	}

	// No column list: values must match schema order.
	if len(stmt.Columns) == 0 {
		if len(stmt.Values) != len(cols) {
			return fmt.Errorf("INSERT: value count %d does not match table columns %d",
				len(stmt.Values), len(cols))
		}
		return tx.Insert(stmt.TableName, stmt.Values)
	}

	// Column list present; must specify all columns for now.
	if len(stmt.Columns) != len(cols) {
		return fmt.Errorf("INSERT: for now, all columns must be specified in column list (have %d, expected %d)",
			len(stmt.Columns), len(cols))
	}
	if len(stmt.Values) != len(stmt.Columns) {
		return fmt.Errorf("INSERT: number of values %d does not match number of columns %d",
			len(stmt.Values), len(stmt.Columns))
	}

	// Map name -> index in table schema
	colIndex := make(map[string]int, len(cols))
	for i, name := range cols {
		colIndex[name] = i
	}

	out := make(sql.Row, len(cols))
	seen := make([]bool, len(cols))

	for i, colName := range stmt.Columns {
		pos, ok := colIndex[colName]
		if !ok {
			return fmt.Errorf("INSERT: unknown column %q", colName)
		}
		if seen[pos] {
			return fmt.Errorf("INSERT: duplicate column %q in column list", colName)
		}
		out[pos] = stmt.Values[i]
		seen[pos] = true
	}

	for i, s := range seen {
		if !s {
			return fmt.Errorf("INSERT: no value provided for column %q", cols[i])
		}
	}

	return tx.Insert(stmt.TableName, out)
}

func (e *DBEngine) beginTx() error {
	if !e.started {
		return fmt.Errorf("engine not started")
	}
	if e.inTx {
		return fmt.Errorf("transaction already in progress")
	}

	tx, err := e.store.Begin(false) // writeable transaction
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	e.currTx = tx
	e.inTx = true
	return nil
}

func (e *DBEngine) commitTx() error {
	if !e.inTx {
		return fmt.Errorf("no active transaction to commit")
	}

	if err := e.store.Commit(e.currTx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	e.currTx = nil
	e.inTx = false
	return nil
}

func (e *DBEngine) rollbackTx() error {
	if !e.inTx {
		return fmt.Errorf("no active transaction to rollback")
	}

	if err := e.store.Rollback(e.currTx); err != nil {
		return fmt.Errorf("rollback tx: %w", err)
	}

	e.currTx = nil
	e.inTx = false
	return nil
}
func (e *DBEngine) selectAllInTx(tx storage.Tx, table string) ([]string, []sql.Row, error) {
	cols, rows, err := tx.Scan(table)
	if err != nil {
		return nil, nil, fmt.Errorf("scan: %w", err)
	}
	return cols, rows, nil
}
