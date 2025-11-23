package sql

import (
	"fmt"
	"strings"
)

// parseUpdate parses:
//
//	UPDATE tableName SET col1 = value1, col2 = value2 WHERE column = literal;
func parseUpdate(query string) (Statement, error) {
	q := strings.TrimSpace(query)

	upper := strings.ToUpper(q)
	if !strings.HasPrefix(upper, "UPDATE ") {
		return nil, fmt.Errorf("UPDATE: expected UPDATE")
	}

	// strip "UPDATE"
	rest := strings.TrimSpace(q[len("UPDATE"):])

	upperRest := strings.ToUpper(rest)

	// find SET
	idxSet := strings.Index(upperRest, "SET")
	if idxSet == -1 {
		return nil, fmt.Errorf("UPDATE: missing SET")
	}

	tableNamePart := strings.TrimSpace(rest[:idxSet])
	if tableNamePart == "" {
		return nil, fmt.Errorf("UPDATE: missing table name")
	}

	afterSet := strings.TrimSpace(rest[idxSet+len("SET"):])
	if afterSet == "" {
		return nil, fmt.Errorf("UPDATE: missing assignments after SET")
	}

	upperAfterSet := strings.ToUpper(afterSet)
	idxWhere := strings.Index(upperAfterSet, "WHERE")

	var assignsPart string
	var wherePart string

	if idxWhere == -1 {
		// For now, require WHERE to avoid accidental full-table updates.
		return nil, fmt.Errorf("UPDATE: WHERE clause required for now")
	} else {
		assignsPart = strings.TrimSpace(afterSet[:idxWhere])
		wherePart = strings.TrimSpace(afterSet[idxWhere+len("WHERE"):])
		if assignsPart == "" {
			return nil, fmt.Errorf("UPDATE: empty SET assignments")
		}
		if wherePart == "" {
			return nil, fmt.Errorf("UPDATE: empty WHERE clause")
		}
	}

	// Parse assignments: "col1 = val1, col2 = val2"
	assignDefs := splitCommaSeparated(assignsPart)
	if len(assignDefs) == 0 {
		return nil, fmt.Errorf("UPDATE: no assignments found")
	}

	assignments := make([]Assignment, 0, len(assignDefs))

	for _, def := range assignDefs {
		def = strings.TrimSpace(def)
		if def == "" {
			continue
		}

		idxEq := strings.Index(def, "=")
		if idxEq == -1 {
			return nil, fmt.Errorf("UPDATE: expected '=' in assignment %q", def)
		}

		colPart := strings.TrimSpace(def[:idxEq])
		valPart := strings.TrimSpace(def[idxEq+1:])

		if colPart == "" || valPart == "" {
			return nil, fmt.Errorf("UPDATE: invalid assignment %q", def)
		}

		val, err := parseLiteral(valPart)
		if err != nil {
			return nil, fmt.Errorf("UPDATE: invalid literal %q: %w", valPart, err)
		}

		assignments = append(assignments, Assignment{
			Column: colPart,
			Value:  val,
		})
	}

	if len(assignments) == 0 {
		return nil, fmt.Errorf("UPDATE: no valid assignments")
	}

	whereExpr, err := parseWhereClause(wherePart)
	if err != nil {
		return nil, err
	}

	return &UpdateStmt{
		TableName:   tableNamePart,
		Assignments: assignments,
		Where:       whereExpr,
	}, nil
}
