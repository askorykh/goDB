package sql

import (
	"fmt"
	"strconv"
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
	q := strings.TrimSpace(query)
	if q == "" {
		return nil, fmt.Errorf("SELECT: empty query")
	}
	// strip trailing ';'
	if strings.HasSuffix(q, ";") {
		q = strings.TrimSpace(q[:len(q)-1])
	}

	upper := strings.ToUpper(q)
	if !strings.HasPrefix(upper, "SELECT") {
		return nil, fmt.Errorf("SELECT: expected SELECT")
	}

	// Find FROM (case-insensitive).
	idxFrom := strings.Index(upper, "FROM")
	if idxFrom == -1 {
		return nil, fmt.Errorf("SELECT: FROM not found")
	}

	// Part between SELECT and FROM => projection list.
	selectPart := strings.TrimSpace(q[len("SELECT"):idxFrom])
	if selectPart == "" {
		return nil, fmt.Errorf("SELECT: missing projection list")
	}

	var cols []string
	if selectPart == "*" {
		cols = nil // SELECT * => nil/empty means "all columns"
	} else {
		colDefs := splitCommaSeparated(selectPart)
		if len(colDefs) == 0 {
			return nil, fmt.Errorf("SELECT: no valid column names")
		}
		for _, c := range colDefs {
			c = strings.TrimSpace(c)
			if c != "" {
				cols = append(cols, c)
			}
		}
		if len(cols) == 0 {
			return nil, fmt.Errorf("SELECT: no valid column names")
		}
	}

	// Everything after FROM: "table [WHERE ...] [ORDER BY ...] [LIMIT ...]"
	rest := strings.TrimSpace(q[idxFrom+len("FROM"):])
	if rest == "" {
		return nil, fmt.Errorf("SELECT: missing table name")
	}

	// Extract table name (first token).
	tableFields := strings.Fields(rest)
	if len(tableFields) == 0 {
		return nil, fmt.Errorf("SELECT: missing table name")
	}
	tableName := tableFields[0]

	// Compute remaining tail after the table name.
	idxTable := strings.Index(rest, tableName)
	if idxTable == -1 {
		return nil, fmt.Errorf("SELECT: internal error parsing table name")
	}
	tail := strings.TrimSpace(rest[idxTable+len(tableName):])

	var whereExpr *WhereExpr
	var orderBy *OrderByClause
	var limitVal *int

	// 1) Optional WHERE ...
	if tail != "" {
		upperTail := strings.ToUpper(tail)
		if strings.HasPrefix(upperTail, "WHERE ") {
			wherePartAndRest := strings.TrimSpace(tail[len("WHERE "):])
			upperWR := strings.ToUpper(wherePartAndRest)

			// WHERE ... [ORDER BY ...] [LIMIT ...]
			// split WHERE clause from possible ORDER BY / LIMIT
			idxOrder := strings.Index(upperWR, " ORDER BY ")
			idxLimit := strings.Index(upperWR, " LIMIT ")

			endWhere := len(wherePartAndRest)
			if idxOrder != -1 && idxOrder < endWhere {
				endWhere = idxOrder
			}
			if idxLimit != -1 && idxLimit < endWhere {
				endWhere = idxLimit
			}

			wherePart := strings.TrimSpace(wherePartAndRest[:endWhere])
			if wherePart == "" {
				return nil, fmt.Errorf("SELECT: empty WHERE clause")
			}

			w, err := parseWhereClause(wherePart)
			if err != nil {
				return nil, err
			}
			whereExpr = w

			// tail becomes whatever comes after WHERE clause.
			tail = strings.TrimSpace(wherePartAndRest[endWhere:])
		}
	}

	// 2) Optional ORDER BY ...
	if tail != "" {
		upperTail := strings.ToUpper(tail)
		if strings.HasPrefix(upperTail, "ORDER BY ") {
			orderPartAndRest := strings.TrimSpace(tail[len("ORDER BY "):])
			upperOR := strings.ToUpper(orderPartAndRest)

			// ORDER BY ... [LIMIT ...]
			idxLimit := strings.Index(upperOR, " LIMIT ")

			endOrder := len(orderPartAndRest)
			if idxLimit != -1 && idxLimit < endOrder {
				endOrder = idxLimit
			}

			orderPart := strings.TrimSpace(orderPartAndRest[:endOrder])
			if orderPart == "" {
				return nil, fmt.Errorf("SELECT: empty ORDER BY clause")
			}

			parts := strings.Fields(orderPart)
			if len(parts) == 0 {
				return nil, fmt.Errorf("SELECT: invalid ORDER BY clause")
			}
			orderCol := parts[0]
			desc := false
			if len(parts) >= 2 {
				dir := strings.ToUpper(parts[1])
				if dir == "DESC" {
					desc = true
				} else if dir != "ASC" {
					return nil, fmt.Errorf("SELECT: ORDER BY direction must be ASC or DESC, got %q", parts[1])
				}
			}

			orderBy = &OrderByClause{
				Column: orderCol,
				Desc:   desc,
			}

			tail = strings.TrimSpace(orderPartAndRest[endOrder:])
		}
	}

	// 3) Optional LIMIT ...
	if tail != "" {
		upperTail := strings.ToUpper(tail)
		if strings.HasPrefix(upperTail, "LIMIT ") {
			limitPart := strings.TrimSpace(tail[len("LIMIT "):])
			if limitPart == "" {
				return nil, fmt.Errorf("SELECT: empty LIMIT value")
			}
			n, err := strconv.Atoi(limitPart)
			if err != nil || n < 0 {
				return nil, fmt.Errorf("SELECT: invalid LIMIT value %q", limitPart)
			}
			limitVal = &n
			tail = ""
		}
	}

	// Any leftover tokens we didn't understand?
	if strings.TrimSpace(tail) != "" {
		return nil, fmt.Errorf("SELECT: unexpected trailing input %q", tail)
	}

	return &SelectStmt{
		TableName: tableName,
		Columns:   cols,
		Where:     whereExpr,
		OrderBy:   orderBy,
		Limit:     limitVal,
	}, nil
}

// parseWhereClause parses a simple binary comparison:
//
//	column = literal
//	column != literal
//	column < literal
//	column <= literal
//	column > literal
//	column >= literal
//
// We keep it deliberately simple and do not support AND/OR yet.
func parseWhereClause(s string) (*WhereExpr, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, fmt.Errorf("WHERE: empty clause")
	}

	upper := strings.ToUpper(s)

	// Order is important: multi-char operators first.
	ops := []string{">=", "<=", "!=", "=", ">", "<"}

	var op string
	var idx = -1

	for _, candidate := range ops {
		i := strings.Index(upper, candidate)
		if i != -1 {
			op = candidate
			idx = i
			break
		}
	}

	if idx == -1 {
		return nil, fmt.Errorf("WHERE: could not find comparison operator in %q", s)
	}

	left := strings.TrimSpace(s[:idx])
	right := strings.TrimSpace(s[idx+len(op):])

	if left == "" || right == "" {
		return nil, fmt.Errorf("WHERE: invalid expression %q", s)
	}

	val, err := parseLiteral(right)
	if err != nil {
		return nil, fmt.Errorf("WHERE: invalid literal %q: %w", right, err)
	}

	return &WhereExpr{
		Column: left,
		Op:     op,
		Value:  val,
	}, nil
}
