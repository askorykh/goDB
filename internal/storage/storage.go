package storage

import "goDB/internal/sql"

type RowPredicate func(row sql.Row) (bool, error)
type RowUpdater func(row sql.Row) (sql.Row, error)

// Tx represents a storage-level transaction.
//
// For now, it only supports inserting rows into a table.
// Later, we'll extend it with Scan, Update, Delete, index lookups, etc.
type Tx interface {
	Insert(tableName string, row sql.Row) error

	Scan(tableName string) (col []string, rows []sql.Row, err error)

	// ReplaceAll replaces the entire rowset of a table.
	// Used for simple UPDATE/DELETE implementations in the engine.
	ReplaceAll(tableName string, rows []sql.Row) error

	DeleteWhere(tableName string, pred RowPredicate) error
	UpdateWhere(tableName string, pred RowPredicate, updater RowUpdater) error
}

// Engine is a storage engine that can create and manage transactions.
//
// Different implementations are possible:
//   - in-memory (for learning & tests)
//   - on-disk with pages and WAL
//   - remote/distributed in the future
type Engine interface {
	// Begin starts a new transaction.
	// readOnly = true means the transaction must not perform writes.
	Begin(readOnly bool) (Tx, error)

	// Commit finishes a transaction and makes changes durable/visible.
	Commit(tx Tx) error

	// Rollback aborts a transaction and discards its changes.
	Rollback(tx Tx) error

	// CreateTable creates a new empty table with the given column names.
	// For now, we only support simple "name + list of columns".
	CreateTable(name string, cols []sql.Column) error

	// ListTables returns the names of all tables in the engine.
	ListTables() ([]string, error)

	// TableSchema returns the column definitions for a table.
	TableSchema(name string) ([]sql.Column, error)
}
