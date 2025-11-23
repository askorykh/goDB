package engine

import (
	"fmt"
	"goDB/internal/sql"
	"sort"
)

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
			fullRows = filterRowsWhere(fullCols, fullRows, s.Where)
		}

		// ORDER BY
		if s.OrderBy != nil {
			sortRows(fullCols, fullRows, s.OrderBy)
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

func sortRows(cols []string, rows []sql.Row, ob *sql.OrderByClause) {
	colIndex := make(map[string]int, len(cols))
	for i, name := range cols {
		colIndex[name] = i
	}
	idx, ok := colIndex[ob.Column]
	if !ok {
		// Unknown ORDER BY column: do nothing (or log)
		return
	}

	sort.SliceStable(rows, func(i, j int) bool {
		a := rows[i][idx]
		b := rows[j][idx]
		cmp, err := compareValues(a, b)
		if err != nil {
			return false
		}
		if ob.Desc {
			return cmp > 0
		}
		return cmp < 0
	})
}
