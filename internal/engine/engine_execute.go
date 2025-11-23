package engine

import (
	"fmt"
	"goDB/internal/sql"
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
		err := e.InsertRow(s.TableName, s.Values)
		return nil, nil, err

	case *sql.SelectStmt:
		cols, rows, err := e.SelectAll(s.TableName)
		if err != nil {
			return nil, nil, err
		}

		// Apply WHERE filter if present.
		if s.Where != nil {
			rows = filterRowsWhere(cols, rows, s.Where)
		}

		return cols, rows, nil

	default:
		return nil, nil, fmt.Errorf("unsupported statement type %T", stmt)
	}
}
