package sql

import (
	"fmt"
	"strings"
)

// parseDelete parses:
//
//	DELETE FROM tableName WHERE column = literal;
func parseDelete(query string) (Statement, error) {
	q := strings.TrimSpace(query)
	upper := strings.ToUpper(q)

	if !strings.HasPrefix(upper, "DELETE") {
		return nil, fmt.Errorf("DELETE: expected DELETE")
	}

	// Expect "DELETE FROM ..."
	// Remove "DELETE"
	rest := strings.TrimSpace(q[len("DELETE"):])
	if rest == "" {
		return nil, fmt.Errorf("DELETE: missing FROM")
	}

	upperRest := strings.ToUpper(rest)
	if !strings.HasPrefix(upperRest, "FROM ") {
		return nil, fmt.Errorf("DELETE: expected FROM after DELETE")
	}

	afterFrom := strings.TrimSpace(rest[len("FROM"):])
	if afterFrom == "" {
		return nil, fmt.Errorf("DELETE: missing table name")
	}

	upperAfter := strings.ToUpper(afterFrom)
	idxWhere := strings.Index(upperAfter, "WHERE")

	var tableName string
	var wherePart string

	if idxWhere == -1 {
		// for safety, require WHERE
		return nil, fmt.Errorf("DELETE: WHERE clause required for now")
	} else {
		tableNameStr := strings.TrimSpace(afterFrom[:idxWhere])
		wherePart = strings.TrimSpace(afterFrom[idxWhere+len("WHERE"):])

		if tableNameStr == "" {
			return nil, fmt.Errorf("DELETE: missing table name before WHERE")
		}
		if wherePart == "" {
			return nil, fmt.Errorf("DELETE: empty WHERE clause")
		}

		toks := strings.Fields(tableNameStr)
		tableName = toks[0]
	}

	whereExpr, err := parseWhereClause(wherePart)
	if err != nil {
		return nil, err
	}

	return &DeleteStmt{
		TableName: tableName,
		Where:     whereExpr,
	}, nil
}
