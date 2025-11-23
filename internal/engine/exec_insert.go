package engine

import (
	"fmt"
	"goDB/internal/sql"
	"goDB/internal/storage"
)

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
