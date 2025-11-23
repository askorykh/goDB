package sql

import (
	"fmt"
	"strings"
)

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
