package sql

import (
	"fmt"
	"strings"
)

// parseInsert parses:
//
//	INSERT INTO table VALUES (v1, v2, ...);
//	INSERT INTO table(col1, col2) VALUES (v1, v2, ...);
func parseInsert(query string) (Statement, error) {
	q := strings.TrimSpace(query)
	if q == "" {
		return nil, fmt.Errorf("INSERT: empty query")
	}

	upper := strings.ToUpper(q)
	tokens := strings.Fields(upper)
	if len(tokens) < 3 || tokens[0] != "INSERT" || tokens[1] != "INTO" {
		return nil, fmt.Errorf("INSERT: expected INSERT INTO")
	}

	// Find "INTO" in the string to get the rest after it.
	idxInto := strings.Index(upper, "INTO")
	if idxInto == -1 {
		return nil, fmt.Errorf("INSERT: expected INTO")
	}
	rest := strings.TrimSpace(q[idxInto+len("INTO"):])
	if rest == "" {
		return nil, fmt.Errorf("INSERT: missing table name")
	}

	upperRest := strings.ToUpper(rest)
	idxValues := strings.Index(upperRest, "VALUES")
	if idxValues == -1 {
		return nil, fmt.Errorf("INSERT: missing VALUES keyword")
	}

	// part before VALUES: "table", or "table(col1, col2)"
	beforeValues := strings.TrimSpace(rest[:idxValues])
	afterValues := strings.TrimSpace(rest[idxValues+len("VALUES"):])
	if afterValues == "" {
		return nil, fmt.Errorf("INSERT: missing VALUES list")
	}

	var tableName string
	var columnList []string

	// Detect column list by looking for '(' in beforeValues.
	if openParen := strings.Index(beforeValues, "("); openParen == -1 {
		// No column list: entire beforeValues is table name.
		tableName = strings.TrimSpace(beforeValues)
		if tableName == "" {
			return nil, fmt.Errorf("INSERT: missing table name")
		}
	} else {
		// Column list present: "tableName(col1, col2...)"
		tableName = strings.TrimSpace(beforeValues[:openParen])

		closeParen := strings.LastIndex(beforeValues, ")")
		if closeParen == -1 || closeParen <= openParen {
			return nil, fmt.Errorf("INSERT: missing closing parenthesis in column list")
		}

		colsStr := strings.TrimSpace(beforeValues[openParen+1 : closeParen])
		if colsStr == "" {
			return nil, fmt.Errorf("INSERT: empty column list")
		}
		rawCols := splitCommaSeparated(colsStr)
		for _, c := range rawCols {
			c = strings.TrimSpace(c)
			if c != "" {
				columnList = append(columnList, c)
			}
		}
		if len(columnList) == 0 {
			return nil, fmt.Errorf("INSERT: no valid column names")
		}
	}

	// Parse VALUES part: must be "( ... )"
	if !strings.HasPrefix(afterValues, "(") || !strings.HasSuffix(afterValues, ")") {
		return nil, fmt.Errorf("INSERT: VALUES must be in parentheses")
	}

	inner := strings.TrimSpace(afterValues[1 : len(afterValues)-1])
	if inner == "" {
		return nil, fmt.Errorf("INSERT: empty VALUES list")
	}

	rawVals := splitCommaSeparated(inner)
	values := make([]Value, 0, len(rawVals))
	for _, rv := range rawVals {
		rv = strings.TrimSpace(rv)
		if rv == "" {
			continue
		}
		v, err := parseLiteral(rv)
		if err != nil {
			return nil, fmt.Errorf("INSERT: invalid literal %q: %w", rv, err)
		}
		values = append(values, v)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("INSERT: no values parsed")
	}

	return &InsertStmt{
		TableName: tableName,
		Columns:   columnList, // nil/empty means no column list
		Values:    Row(values),
	}, nil
}
