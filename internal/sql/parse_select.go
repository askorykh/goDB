package sql

import (
	"fmt"
	"strings"
)

// parseSelect parses a simple SELECT statement.
//
// Supported forms (case-insensitive, with flexible spaces):
//
//	SELECT * FROM users;
//	SELECT id, name FROM users;
//	SELECT id, name FROM users WHERE active = true;
func parseSelect(query string) (Statement, error) {
	// query is trimmed and has no trailing semicolon here.

	upper := strings.ToUpper(query)
	if !strings.HasPrefix(strings.TrimSpace(upper), "SELECT") {
		return nil, fmt.Errorf("SELECT: expected SELECT")
	}

	// Find FROM (case-insensitive).
	idxFrom := strings.Index(upper, "FROM")
	if idxFrom == -1 {
		return nil, fmt.Errorf("SELECT: FROM not found")
	}

	// Part between SELECT and FROM: projection list (* or col list).
	// Example: "SELECT *   " or "SELECT id, name  "
	selectPart := strings.TrimSpace(query[len("SELECT"):idxFrom])
	if selectPart == "" {
		return nil, fmt.Errorf("SELECT: missing projection list")
	}

	var cols []string
	if selectPart == "*" {
		// nil or empty slice means "all columns"
		cols = nil
	} else {
		// Split "id, name, active" -> ["id", "name", "active"]
		colDefs := splitCommaSeparated(selectPart)
		if len(colDefs) == 0 {
			return nil, fmt.Errorf("SELECT: no valid column names")
		}
		cols = make([]string, 0, len(colDefs))
		for _, c := range colDefs {
			c = strings.TrimSpace(c)
			if c == "" {
				continue
			}
			cols = append(cols, c)
		}
		if len(cols) == 0 {
			return nil, fmt.Errorf("SELECT: no valid column names")
		}
	}

	// Everything after FROM: "users WHERE ..." or just "users"
	afterFrom := strings.TrimSpace(query[idxFrom+len("FROM"):])
	if afterFrom == "" {
		return nil, fmt.Errorf("SELECT: missing table name")
	}

	upperAfter := strings.ToUpper(afterFrom)
	idxWhere := strings.Index(upperAfter, "WHERE")

	var tableName string
	var wherePart string

	if idxWhere == -1 {
		// No WHERE: entire rest is table name
		tableNameStr := strings.TrimSpace(afterFrom)
		toks := strings.Fields(tableNameStr)
		if len(toks) == 0 {
			return nil, fmt.Errorf("SELECT: missing table name")
		}
		tableName = toks[0]
	} else {
		// Split "table" and "where ..."
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
		Columns:   cols, // nil/empty => SELECT *
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
