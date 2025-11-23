package engine

import "goDB/internal/sql"

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
