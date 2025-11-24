package sql

import (
	"fmt"
	"strings"
)

// parseCreateIndex parses a CREATE INDEX statement.
// Format: CREATE INDEX index_name ON table_name (column_name)
func parseCreateIndex(q string) (*CreateIndexStmt, error) {
	q = strings.TrimSpace(q)
	parts := strings.Fields(q)

	if len(parts) != 6 ||
		!strings.EqualFold(parts[0], "CREATE") ||
		!strings.EqualFold(parts[1], "INDEX") ||
		!strings.EqualFold(parts[3], "ON") ||
		!strings.HasPrefix(parts[5], "(") ||
		!strings.HasSuffix(parts[5], ")") {
		return nil, fmt.Errorf("invalid CREATE INDEX format")
	}

	stmt := &CreateIndexStmt{
		IndexName:  parts[2],
		TableName:  parts[4],
		ColumnName: strings.Trim(parts[5], "()"),
	}

	return stmt, nil
}
