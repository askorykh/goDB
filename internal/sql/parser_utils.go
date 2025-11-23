package sql

import (
	"fmt"
	"strconv"
	"strings"
)

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

	if upper == "NULL" || upper == "DEFAULT" {
		return Value{Type: TypeNull}, nil
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
