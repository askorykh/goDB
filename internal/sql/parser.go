package sql

import (
	"fmt"
	"strings"
)

// Parse parses a single SQL statement string into an AST Statement.
// For now it only supports CREATE TABLE statements.
func Parse(query string) (Statement, error) {
	// Trim spaces and optional trailing semicolon.
	q := strings.TrimSpace(query)
	if strings.HasSuffix(q, ";") {
		q = strings.TrimSpace(q[:len(q)-1])
	}

	// Case-insensitive check for "CREATE TABLE".
	up := strings.ToUpper(q)
	if strings.HasPrefix(up, "CREATE TABLE ") {
		return parseCreateTable(q)
	}

	return nil, fmt.Errorf("unsupported statement (only CREATE TABLE is supported for now)")
}

func parseCreateTable(query string) (Statement, error) {
	// We expect something like:
	// CREATE TABLE users (id INT, name STRING, active BOOL)

	// Strip trailing semicolon handled in Parse, so here we just parse structure.

	// Find "CREATE TABLE" (case-insensitive), then get rest.
	up := strings.ToUpper(query)

	const kw = "CREATE TABLE "
	idx := strings.Index(up, kw)
	if idx != 0 {
		return nil, fmt.Errorf("invalid CREATE TABLE syntax")
	}

	rest := strings.TrimSpace(query[len(kw):])

	// Find the opening parenthesis for column list.
	openIdx := strings.Index(rest, "(")
	if openIdx == -1 {
		return nil, fmt.Errorf("CREATE TABLE: missing '('")
	}

	tableNamePart := strings.TrimSpace(rest[:openIdx])
	if tableNamePart == "" {
		return nil, fmt.Errorf("CREATE TABLE: missing table name")
	}

	// Extract column definitions between '(' and ')'.
	closeIdx := strings.LastIndex(rest, ")")
	if closeIdx == -1 || closeIdx <= openIdx {
		return nil, fmt.Errorf("CREATE TABLE: missing or misplaced ')'")
	}

	colsPart := strings.TrimSpace(rest[openIdx+1 : closeIdx])
	if colsPart == "" {
		return nil, fmt.Errorf("CREATE TABLE: no column definitions")
	}

	// Split by comma into individual column definitions.
	colDefs := splitCommaSeparated(colsPart)

	columns := make([]Column, 0, len(colDefs))

	for _, def := range colDefs {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}

		parts := strings.Fields(def)
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid column definition: %q", def)
		}

		colName := parts[0]
		typeStr := strings.ToUpper(parts[1])

		var dt DataType
		switch typeStr {
		case "INT", "INTEGER":
			dt = TypeInt
		case "FLOAT", "DOUBLE", "REAL":
			dt = TypeFloat
		case "STRING", "TEXT", "VARCHAR":
			dt = TypeString
		case "BOOL", "BOOLEAN":
			dt = TypeBool
		default:
			return nil, fmt.Errorf("unknown column type %q in %q", typeStr, def)
		}

		columns = append(columns, Column{
			Name: colName,
			Type: dt,
		})
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("CREATE TABLE: no valid columns")
	}

	stmt := &CreateTableStmt{
		TableName: tableNamePart,
		Columns:   columns,
	}
	return stmt, nil
}

// splitCommaSeparated splits a string by commas, but keeps it simple:
// it's fine for "id INT, name STRING, active BOOL".
func splitCommaSeparated(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
