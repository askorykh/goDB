package sql

import (
	"fmt"
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
	if len(tokens) == 0 {
		return nil, fmt.Errorf("invalid SQL statement")
	}

	switch tokens[0] {
	case "CREATE":
		if len(tokens) >= 2 && tokens[1] == "TABLE" {
			return parseCreateTable(q)
		}
		return nil, fmt.Errorf("invalid SQL statement")
	case "INSERT":
		if len(tokens) >= 2 && tokens[1] == "INTO" {
			return parseInsert(q)
		}
		return nil, fmt.Errorf("invalid SQL statement")
	case "SELECT":
		return parseSelect(q)
	case "UPDATE":
		return parseUpdate(q)
	case "DELETE":
		return parseDelete(q)
	case "BEGIN":
		return parseBegin(q)
	case "COMMIT":
		return parseCommit(q)
	case "ROLLBACK":
		return parseRollback(q)
	default:
		return nil, fmt.Errorf("invalid SQL statement")
	}

	return nil, fmt.Errorf("unsupported statement (supported: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, BEGIN, COMMIT, ROLLBACK)")

}
