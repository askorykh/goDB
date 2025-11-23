package sql

import (
	"fmt"
	"strings"
)

func parseBegin(query string) (Statement, error) {
	q := strings.TrimSpace(query)

	// strip trailing semicolon if present
	if strings.HasSuffix(q, ";") {
		q = strings.TrimSpace(q[:len(q)-1])
	}

	upper := strings.ToUpper(q)

	// Allow: BEGIN or BEGIN TRANSACTION
	if upper == "BEGIN" || upper == "BEGIN TRANSACTION" {
		return &BeginTxStmt{}, nil
	}
	if strings.HasPrefix(upper, "BEGIN ") {
		return nil, fmt.Errorf("BEGIN: only 'BEGIN' or 'BEGIN TRANSACTION' are supported")
	}
	return nil, fmt.Errorf("BEGIN: invalid syntax")
}

func parseCommit(query string) (Statement, error) {
	q := strings.TrimSpace(query)
	if strings.HasSuffix(q, ";") {
		q = strings.TrimSpace(q[:len(q)-1])
	}
	upper := strings.ToUpper(q)

	if upper == "COMMIT" || upper == "COMMIT TRANSACTION" {
		return &CommitTxStmt{}, nil
	}
	if strings.HasPrefix(upper, "COMMIT ") {
		return nil, fmt.Errorf("COMMIT: only 'COMMIT' or 'COMMIT TRANSACTION' are supported")
	}
	return nil, fmt.Errorf("COMMIT: invalid syntax")
}

func parseRollback(query string) (Statement, error) {
	q := strings.TrimSpace(query)
	if strings.HasSuffix(q, ";") {
		q = strings.TrimSpace(q[:len(q)-1])
	}
	upper := strings.ToUpper(q)

	if upper == "ROLLBACK" || upper == "ROLLBACK TRANSACTION" {
		return &RollbackTxStmt{}, nil
	}
	if strings.HasPrefix(upper, "ROLLBACK ") {
		return nil, fmt.Errorf("ROLLBACK: only 'ROLLBACK' or 'ROLLBACK TRANSACTION' are supported")
	}
	return nil, fmt.Errorf("ROLLBACK: invalid syntax")
}
