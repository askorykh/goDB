package sql

import (
	"fmt"
	"strings"
)

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
