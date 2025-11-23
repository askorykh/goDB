package engine

import (
	"fmt"
	"goDB/internal/sql"
	"strings"
)

// applyUpdate returns a new rowset where all rows matching WHERE are updated
// according to assignments. It returns the updated rows and the count of affected
// rows. Column lookups are resolved once up front to avoid repeated map access
// inside the loop.
func applyUpdate(cols []string, rows []sql.Row, where *sql.WhereExpr, assigns []sql.Assignment) ([]sql.Row, int, error) {
	colIndex := make(map[string]int, len(cols))
	for i, name := range cols {
		colIndex[strings.ToLower(name)] = i
	}

	whereIdx, ok := colIndex[strings.ToLower(where.Column)]
	if !ok {
		return nil, 0, fmt.Errorf("UPDATE: unknown column %q in WHERE", where.Column)
	}

	assignIdx := make([]int, len(assigns))
	for i, a := range assigns {
		idx, ok := colIndex[strings.ToLower(a.Column)]
		if !ok {
			return nil, 0, fmt.Errorf("UPDATE: unknown column %q in SET list", a.Column)
		}
		assignIdx[i] = idx
	}

	newRows := make([]sql.Row, len(rows))
	affected := 0

	for i, r := range rows {
		newRow := make(sql.Row, len(r))
		copy(newRow, r)

		if conditionMatches(newRow[whereIdx], where.Op, where.Value) {
			for j, a := range assigns {
				idx := assignIdx[j]
				newRow[idx] = a.Value
			}
			affected++
		}

		newRows[i] = newRow
	}

	return newRows, affected, nil
}

// applyDelete returns a new rowset where all rows matching WHERE are removed.
// It returns the new rows and the count of deleted rows.
func applyDelete(cols []string, rows []sql.Row, where *sql.WhereExpr) ([]sql.Row, int, error) {
	colIndex := make(map[string]int, len(cols))
	for i, name := range cols {
		colIndex[strings.ToLower(name)] = i
	}

	whereIdx, ok := colIndex[strings.ToLower(where.Column)]
	if !ok {
		return nil, 0, fmt.Errorf("DELETE: unknown column %q in WHERE", where.Column)
	}

	out := make([]sql.Row, 0, len(rows))
	deleted := 0

	for _, r := range rows {
		if whereIdx < 0 || whereIdx >= len(r) {
			out = append(out, r)
			continue
		}
		if conditionMatches(r[whereIdx], where.Op, where.Value) {
			deleted++
			continue
		}
		out = append(out, r)
	}

	return out, deleted, nil
}
