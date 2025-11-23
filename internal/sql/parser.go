package sql

import (
	"fmt"
	"strconv"
	"strings"
)

// Parse parses a single SQL statement string into an AST Statement.
// For now it only supports CREATE TABLE statements.
func Parse(query string) (Statement, error) {
	// Trim leading & trailing whitespace
	q := strings.TrimSpace(query)
	if q == "" {
		return nil, fmt.Errorf("empty query")
	}

	// Remove trailing semicolon if present
	if strings.HasSuffix(q, ";") {
		q = strings.TrimSpace(q[:len(q)-1])
	}

	upper := strings.ToUpper(q)
	tokens := strings.Fields(upper)
	if len(tokens) < 2 {
		return nil, fmt.Errorf("invalid SQL statement")
	}

	switch tokens[0] {
	case "CREATE":
		if len(tokens) >= 2 && tokens[1] == "TABLE" {
			return parseCreateTable(q)
		}
	case "INSERT":
		if len(tokens) >= 2 && tokens[1] == "INTO" {
			return parseInsert(q)
		}
	}

	return nil, fmt.Errorf("unsupported statement (only CREATE TABLE is supported for now)")
}

func parseCreateTable(query string) (Statement, error) {
	// At this point:
	// - query has been trimmed
	// - trailing ';' removed
	// - we already know it's some form of CREATE TABLE

	// Find the opening parenthesis for column list.
	openIdx := strings.Index(query, "(")
	if openIdx == -1 {
		return nil, fmt.Errorf("CREATE TABLE: missing '('")
	}

	// Find the closing parenthesis.
	closeIdx := strings.LastIndex(query, ")")
	if closeIdx == -1 || closeIdx <= openIdx {
		return nil, fmt.Errorf("CREATE TABLE: missing or misplaced ')'")
	}

	// "head" contains: CREATE   TABLE   Accounts
	head := strings.TrimSpace(query[:openIdx])
	// "colsPart" contains everything between '(' and ')'
	colsPart := strings.TrimSpace(query[openIdx+1 : closeIdx])
	if colsPart == "" {
		return nil, fmt.Errorf("CREATE TABLE: no column definitions")
	}

	// Extract table name from "head".
	// Example: "create   table   Accounts" → ["create", "table", "Accounts"]
	headTokens := strings.Fields(head)
	if len(headTokens) < 3 {
		return nil, fmt.Errorf("CREATE TABLE: missing table name")
	}

	// Basic keyword check (case-insensitive).
	if strings.ToUpper(headTokens[0]) != "CREATE" || strings.ToUpper(headTokens[1]) != "TABLE" {
		return nil, fmt.Errorf("CREATE TABLE: invalid syntax")
	}

	// Table name is the last token (works for simple "CREATE TABLE name").
	tableName := headTokens[len(headTokens)-1]

	// Split column definitions by comma.
	colDefs := splitCommaSeparated(colsPart)
	if len(colDefs) == 0 {
		return nil, fmt.Errorf("CREATE TABLE: no valid columns")
	}

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

	return &CreateTableStmt{
		TableName: tableName,
		Columns:   columns,
	}, nil
}

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

// parseLiteral parses a single literal token into a Value.
// Supports:
//   - integers:  1, 42
//   - floats:    3.14, 1e3
//   - strings:   'Alice'  (single quotes)
//   - booleans:  true / false (case-insensitive)
func parseLiteral(tok string) (Value, error) {
	s := strings.TrimSpace(tok)
	if s == "" {
		return Value{}, fmt.Errorf("empty literal")
	}

	upper := strings.ToUpper(s)

	// Boolean
	if upper == "TRUE" {
		return Value{Type: TypeBool, B: true}, nil
	}
	if upper == "FALSE" {
		return Value{Type: TypeBool, B: false}, nil
	}

	// String literal with single quotes
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		inner := s[1 : len(s)-1]
		// TODO: handle escaped quotes like '' inside string
		return Value{Type: TypeString, S: inner}, nil
	}

	// Try integer
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return Value{Type: TypeInt, I64: i}, nil
	}

	// Try float
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return Value{Type: TypeFloat, F64: f}, nil
	}

	return Value{}, fmt.Errorf("cannot parse literal %q", tok)
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
