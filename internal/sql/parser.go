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
	case "SELECT":
		return parseSelect(q)
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

// parseSelect parses a very simple SELECT statement.
// Supported forms (case-insensitive, flexible spaces):
//
//	SELECT * FROM users;
//	SELECT * FROM users WHERE id = 1;
//	SELECT * FROM users WHERE name = 'Alice';
func parseSelect(query string) (Statement, error) {
	// query is trimmed and has no trailing semicolon here.

	upper := strings.ToUpper(query)

	// Ensure it starts with SELECT * (simplest possible)
	tokens := strings.Fields(upper)
	if len(tokens) < 4 {
		return nil, fmt.Errorf("SELECT: incomplete statement")
	}
	if tokens[0] != "SELECT" {
		return nil, fmt.Errorf("SELECT: expected SELECT")
	}
	if tokens[1] != "*" {
		return nil, fmt.Errorf("SELECT: only SELECT * is supported for now")
	}
	// We don't enforce tokens[2] == "FROM" here; we do it below using string search.

	// Find FROM (case-insensitive) in the original string.
	idxFrom := strings.Index(upper, "FROM")
	if idxFrom == -1 {
		return nil, fmt.Errorf("SELECT: FROM not found")
	}

	beforeFrom := strings.TrimSpace(query[:idxFrom])
	_ = beforeFrom // currently unused; we already validated "SELECT *"

	afterFrom := strings.TrimSpace(query[idxFrom+len("FROM"):])
	if afterFrom == "" {
		return nil, fmt.Errorf("SELECT: missing table name")
	}

	// Check if there's a WHERE clause in the part after FROM.
	upperAfter := strings.ToUpper(afterFrom)
	idxWhere := strings.Index(upperAfter, "WHERE")

	var tableName string
	var wherePart string

	if idxWhere == -1 {
		// No WHERE: the rest is just the table name.
		tableNameStr := strings.TrimSpace(afterFrom)
		toks := strings.Fields(tableNameStr)
		if len(toks) == 0 {
			return nil, fmt.Errorf("SELECT: missing table name")
		}
		tableName = toks[0]
	} else {
		// There is a WHERE: split table name and where clause.
		tableNameStr := strings.TrimSpace(afterFrom[:idxWhere])
		toks := strings.Fields(tableNameStr)
		if len(toks) == 0 {
			return nil, fmt.Errorf("SELECT: missing table name before WHERE")
		}
		tableName = toks[0]

		wherePart = strings.TrimSpace(afterFrom[idxWhere+len("WHERE"):])
		if wherePart == "" {
			return nil, fmt.Errorf("SELECT: empty WHERE clause")
		}
	}

	var where *WhereExpr
	if wherePart != "" {
		w, err := parseWhereClause(wherePart)
		if err != nil {
			return nil, err
		}
		where = w
	}

	return &SelectStmt{
		TableName: tableName,
		Where:     where,
	}, nil
}

// parseWhereClause parses a simple "column = literal" expression.
func parseWhereClause(wherePart string) (*WhereExpr, error) {
	// Expect: column [spaces] = [spaces] literal
	idxEq := strings.Index(wherePart, "=")
	if idxEq == -1 {
		return nil, fmt.Errorf("WHERE: only '=' operator is supported for now")
	}

	colPart := strings.TrimSpace(wherePart[:idxEq])
	valPart := strings.TrimSpace(wherePart[idxEq+1:])

	if colPart == "" {
		return nil, fmt.Errorf("WHERE: missing column name")
	}
	if valPart == "" {
		return nil, fmt.Errorf("WHERE: missing value after '='")
	}

	val, err := parseLiteral(valPart)
	if err != nil {
		return nil, fmt.Errorf("WHERE: invalid literal %q: %w", valPart, err)
	}

	return &WhereExpr{
		Column: colPart,
		Op:     "=",
		Value:  val,
	}, nil
}
