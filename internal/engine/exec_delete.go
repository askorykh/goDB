package engine

import (
	"fmt"
	"goDB/internal/sql"
	"goDB/internal/storage"
)

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
