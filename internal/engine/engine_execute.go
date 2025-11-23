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
		// Get full rowset from storage.
		fullCols, fullRows, err := e.SelectAll(s.TableName)
		if err != nil {
			return nil, nil, err
		}

		// Apply WHERE filter first (uses full column set).
		if s.Where != nil {
			fullRows = filterRowsWhere(fullCols, fullRows, s.Where)
		}

		// If no column list -> return all columns.
		if len(s.Columns) == 0 {
			return fullCols, fullRows, nil
		}

		// Otherwise project only requested columns.
		projCols, projRows, err := projectColumns(fullCols, fullRows, s.Columns)
		return projCols, projRows, err
	case *sql.UpdateStmt:
		return nil, nil, e.executeUpdate(s)

	case *sql.DeleteStmt:
		return nil, nil, e.executeDelete(s)

	default:
		return nil, nil, fmt.Errorf("unsupported statement type %T", stmt)
	}
}
