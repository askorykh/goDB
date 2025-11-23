package engine

import (
	"fmt"
	"goDB/internal/sql"
	"goDB/internal/storage"
)

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
