package engine

import (
	"fmt"
	"goDB/internal/sql"
	"sort"
)

// Execute takes a parsed SQL Statement and executes it using the engine.
//
// It always returns a column header slice and row data slice; for statements
// that are not expected to yield rows (CREATE, INSERT, UPDATE, DELETE, and
// transaction statements) both slices are empty and the caller can treat a
// nil error as success. SELECT statements return the full projected columns
// and rows, applying WHERE/ORDER BY/LIMIT in that order.
func (e *DBEngine) Execute(stmt sql.Statement) ([]string, []sql.Row, error) {
	if !e.started {
		return nil, nil, fmt.Errorf("engine not started")
	}

	switch s := stmt.(type) {
	case *sql.CreateTableStmt:
		err := e.CreateTable(s.TableName, s.Columns)
		return nil, nil, err

	case *sql.InsertStmt:
		return nil, nil, e.executeInsert(s)

	case *sql.SelectStmt:
		var fullCols []string
		var fullRows []sql.Row
		var err error

		if e.inTx {
			fullCols, fullRows, err = e.executeSelectInTx(e.currTx, s.TableName)
		} else {
			fullCols, fullRows, err = e.executeSelect(s.TableName)
		}
		if err != nil {
			return nil, nil, err
		}

		// WHERE
		if s.Where != nil {
			fullRows, err = filterRowsWhere(fullCols, fullRows, s.Where)
			if err != nil {
				return nil, nil, err
			}
		}

		// ORDER BY
		if s.OrderBy != nil {
			if err := sortRows(fullCols, fullRows, s.OrderBy); err != nil {
				return nil, nil, err
			}
		}

		// LIMIT
		if s.Limit != nil {
			n := *s.Limit
			if n < len(fullRows) {
				fullRows = fullRows[:n]
			}
		}

		// Projection
		if len(s.Columns) == 0 {
			return fullCols, fullRows, nil
		}
		projCols, projRows, err := projectColumns(fullCols, fullRows, s.Columns)
		return projCols, projRows, err

	case *sql.UpdateStmt:
		return nil, nil, e.executeUpdate(s)

	case *sql.DeleteStmt:
		return nil, nil, e.executeDelete(s)

	case *sql.BeginTxStmt:
		err := e.beginTx()
		return nil, nil, err

	case *sql.CommitTxStmt:
		err := e.commitTx()
		return nil, nil, err

	case *sql.RollbackTxStmt:
		err := e.rollbackTx()
		return nil, nil, err

	default:
		return nil, nil, fmt.Errorf("unsupported statement type %T", stmt)
	}
}

// sortRows orders the provided rows in place based on the ORDER BY clause.
// It uses a stable sort so rows with equal keys preserve their original
// relative order.
func sortRows(cols []string, rows []sql.Row, ob *sql.OrderByClause) error {
	colIndex := make(map[string]int, len(cols))
	for i, name := range cols {
		colIndex[name] = i
	}
	idx, ok := colIndex[ob.Column]
	if !ok {
		return fmt.Errorf("unknown column %q in ORDER BY", ob.Column)
	}

	sort.SliceStable(rows, func(i, j int) bool {
		a := rows[i][idx]
		b := rows[j][idx]
		cmp, err := compareValues(a, b)
		if err != nil {
			// keep stable ordering on comparison errors
			return false
		}
		if ob.Desc {
			return cmp > 0
		}
		return cmp < 0
	})

	return nil
}
