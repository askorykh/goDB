package engine

import (
	"fmt"
	"goDB/internal/sql"
)

// applyUpdate returns a new rowset where all rows matching WHERE are updated
// according to assignments. It returns the updated rows and the count of affected rows.
func applyUpdate(cols []string, rows []sql.Row, where *sql.WhereExpr, assigns []sql.Assignment) ([]sql.Row, int, error) {
	colIndex := make(map[string]int, len(cols))
	for i, name := range cols {
		colIndex[name] = i
	}

	whereIdx, ok := colIndex[where.Column]
	if !ok {
		// No rows can match; nothing to update.
		return rows, 0, nil
	}

	// Precompute assignment indexes
	assignIdx := make([]int, len(assigns))
	for i, a := range assigns {
		idx, ok := colIndex[a.Column]
		if !ok {
			return nil, 0, fmt.Errorf("UPDATE: unknown column %q in SET list", a.Column)
		}
		assignIdx[i] = idx
	}

	// Copy rows so we don't mutate the original slice
	newRows := make([]sql.Row, len(rows))
	affected := 0

	for i, r := range rows {
		// copy row
		newRow := make(sql.Row, len(r))
		copy(newRow, r)

		// WHERE check
		if valuesEqual(newRow[whereIdx], where.Value) {
			// apply assignments
			for j, a := range assigns {
				idx := assignIdx[j]
				// Optionally: type check is already done by storage.ReplaceAll, but we can be explicit here too.
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
func applyDelete(cols []string, rows []sql.Row, where *sql.WhereExpr) ([]sql.Row, int) {
	colIndex := make(map[string]int, len(cols))
	for i, name := range cols {
		colIndex[name] = i
	}

	whereIdx, ok := colIndex[where.Column]
	if !ok {
		// No rows match; nothing deleted.
		return rows, 0
	}

	var out []sql.Row
	deleted := 0

	for _, r := range rows {
		if whereIdx < 0 || whereIdx >= len(r) {
			// skip weird rows
			out = append(out, r)
			continue
		}
		if valuesEqual(r[whereIdx], where.Value) {
			deleted++
			continue
		}
		out = append(out, r)
	}

	return out, deleted
}
