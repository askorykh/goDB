package sql

import (
	"fmt"
	"strings"
)

// parseInsert parses an INSERT INTO ... VALUES (...) statement.
// Example supported syntax:
//
//	INSERT INTO users VALUES (1, 'Alice', true);
func parseInsert(query string) (Statement, error) {
	// At this point:
	// - query is trimmed
	// - trailing ';' removed

	upper := strings.ToUpper(query)

	// Locate "INTO" and "VALUES" (case-insensitive)
	idxInto := strings.Index(upper, "INTO")
	if idxInto == -1 {
		return nil, fmt.Errorf("INSERT: missing INTO")
	}

	afterInto := strings.TrimSpace(query[idxInto+len("INTO"):])

	upperAfterInto := strings.ToUpper(afterInto)
	idxValues := strings.Index(upperAfterInto, "VALUES")
	if idxValues == -1 {
		return nil, fmt.Errorf("INSERT: missing VALUES")
	}

	tableNamePart := strings.TrimSpace(afterInto[:idxValues])
	if tableNamePart == "" {
		return nil, fmt.Errorf("INSERT: missing table name")
	}

	rest := strings.TrimSpace(afterInto[idxValues+len("VALUES"):])
	if rest == "" {
		return nil, fmt.Errorf("INSERT: missing VALUES list")
	}

	// rest should start with '(' and contain ')'
	if !strings.HasPrefix(rest, "(") {
		return nil, fmt.Errorf("INSERT: expected '(' after VALUES")
	}

	closeIdx := strings.LastIndex(rest, ")")
	if closeIdx == -1 {
		return nil, fmt.Errorf("INSERT: missing closing ')'")
	}

	valuesPart := strings.TrimSpace(rest[1:closeIdx])
	if valuesPart == "" {
		return nil, fmt.Errorf("INSERT: empty VALUES list")
	}

	// Split value expressions by comma.
	rawVals := splitCommaSeparated(valuesPart)

	vals := make([]Value, 0, len(rawVals))
	for _, rv := range rawVals {
		v, err := parseLiteral(rv)
		if err != nil {
			return nil, fmt.Errorf("invalid literal %q: %w", rv, err)
		}
		vals = append(vals, v)
	}

	if len(vals) == 0 {
		return nil, fmt.Errorf("INSERT: no valid values")
	}

	return &InsertStmt{
		TableName: tableNamePart,
		Values:    Row(vals),
	}, nil
}
