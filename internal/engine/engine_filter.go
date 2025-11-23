package engine

import (
	"fmt"
	"goDB/internal/sql"
)

// filterRowsWhere filters rows according to a simple WHERE expression (column = literal).
func filterRowsWhere(cols []string, rows []sql.Row, where *sql.WhereExpr) []sql.Row {
	// Map column name -> index
	colIndex := make(map[string]int, len(cols))
	for i, name := range cols {
		colIndex[name] = i
	}

	idx, ok := colIndex[where.Column]
	if !ok {
		// Column not found: no rows match.
		return nil
	}

	var out []sql.Row
	for _, row := range rows {
		if idx < 0 || idx >= len(row) {
			continue
		}
		v := row[idx]
		if valuesEqual(v, where.Value) {
			out = append(out, row)
		}
	}

	return out
}

// valuesEqual compares two sql.Value for equality, considering their type.
func valuesEqual(a, b sql.Value) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case sql.TypeInt:
		return a.I64 == b.I64
	case sql.TypeFloat:
		return a.F64 == b.F64
	case sql.TypeString:
		return a.S == b.S
	case sql.TypeBool:
		return a.B == b.B
	default:
		return false
	}
}

// projectColumns returns only the requested columns (in that order).
// requestedCols is the list from SELECT (e.g. ["id", "name"]).
func projectColumns(allCols []string, rows []sql.Row, requestedCols []string) ([]string, []sql.Row, error) {
	// Build name -> index map from all columns.
	colIndex := make(map[string]int, len(allCols))
	for i, name := range allCols {
		colIndex[name] = i
	}

	indexes := make([]int, len(requestedCols))
	for i, name := range requestedCols {
		idx, ok := colIndex[name]
		if !ok {
			return nil, nil, fmt.Errorf("unknown column %q in SELECT list", name)
		}
		indexes[i] = idx
	}

	// Project header.
	outCols := make([]string, len(requestedCols))
	copy(outCols, requestedCols)

	// Project each row.
	outRows := make([]sql.Row, 0, len(rows))
	for _, r := range rows {
		proj := make(sql.Row, len(indexes))
		for i, idx := range indexes {
			if idx < 0 || idx >= len(r) {
				return nil, nil, fmt.Errorf("internal error: column index %d out of range", idx)
			}
			proj[i] = r[idx]
		}
		outRows = append(outRows, proj)
	}

	return outCols, outRows, nil
}
