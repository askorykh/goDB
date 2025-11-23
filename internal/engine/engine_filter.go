package engine

import (
	"fmt"
	"goDB/internal/sql"
	"strings"
)

// filterRowsWhere filters rows according to a simple WHERE expression (column = literal).
func filterRowsWhere(cols []string, rows []sql.Row, where *sql.WhereExpr) ([]sql.Row, error) {
	colIndex := make(map[string]int, len(cols))
	for i, name := range cols {
		colIndex[name] = i
	}

	idx, ok := colIndex[where.Column]
	if !ok {
		return nil, fmt.Errorf("unknown column %q in WHERE clause", where.Column)
	}

	out := make([]sql.Row, 0, len(rows))
	for _, r := range rows {
		if idx < 0 || idx >= len(r) {
			continue
		}
		if conditionMatches(r[idx], where.Op, where.Value) {
			out = append(out, r)
		}
	}
	return out, nil
}

// valuesEqual compares two sql.Value for equality, considering their type.
func valuesEqual(a, b sql.Value) bool {
	// If either side is NULL, nothing is equal (even NULL = NULL is false for now).
	if a.Type == sql.TypeNull || b.Type == sql.TypeNull {
		return false
	}
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

// compareValues compares two non-NULL values of the same type.
// Returns -1 if a < b, 0 if a == b, 1 if a > b.
// If types differ or comparison is not meaningful, returns an error.
func compareValues(a, b sql.Value) (int, error) {
	if a.Type == sql.TypeNull || b.Type == sql.TypeNull {
		return 0, fmt.Errorf("cannot compare NULL values")
	}
	if a.Type != b.Type {
		return 0, fmt.Errorf("cannot compare values of different types")
	}

	switch a.Type {
	case sql.TypeInt:
		if a.I64 < b.I64 {
			return -1, nil
		} else if a.I64 > b.I64 {
			return 1, nil
		}
		return 0, nil
	case sql.TypeFloat:
		if a.F64 < b.F64 {
			return -1, nil
		} else if a.F64 > b.F64 {
			return 1, nil
		}
		return 0, nil
	case sql.TypeString:
		return strings.Compare(a.S, b.S), nil
	case sql.TypeBool:
		ai := 0
		if a.B {
			ai = 1
		}
		bi := 0
		if b.B {
			bi = 1
		}
		if ai < bi {
			return -1, nil
		} else if ai > bi {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("unsupported type in compareValues: %v", a.Type)
	}
}

// conditionMatches checks rowValue <op> whereValue.
func conditionMatches(rowVal sql.Value, op string, whereVal sql.Value) bool {
	switch op {
	case "=":
		return valuesEqual(rowVal, whereVal)
	case "!=":
		return !valuesEqual(rowVal, whereVal)
	case "<", "<=", ">", ">=":
		cmp, err := compareValues(rowVal, whereVal)
		if err != nil {
			return false
		}
		switch op {
		case "<":
			return cmp < 0
		case "<=":
			return cmp <= 0
		case ">":
			return cmp > 0
		case ">=":
			return cmp >= 0
		}
	}
	return false
}
