package engine

import (
	"fmt"
	"goDB/internal/sql"
	"goDB/internal/storage"
)

func (e *DBEngine) executeSelectInTx(tx storage.Tx, table string) ([]string, []sql.Row, error) {
	cols, rows, err := tx.Scan(table)
	if err != nil {
		return nil, nil, fmt.Errorf("scan: %w", err)
	}
	return cols, rows, nil
}

// executeSelect returns all rows from the given table.
func (e *DBEngine) executeSelect(tableName string) ([]string, []sql.Row, error) {
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
