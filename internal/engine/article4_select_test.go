package engine

import (
	"testing"

	"goDB/internal/sql"
	"goDB/internal/storage/memstore"
)

// Article 4 coverage: projection, WHERE filtering, comparisons, ORDER BY, and LIMIT.
func TestArticle4_SelectFeatures(t *testing.T) {
	store := memstore.New()
	eng := New(store)

	if err := eng.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	createSQL := "CREATE TABLE users (id INT, name STRING, age INT);"
	stmt, err := sql.Parse(createSQL)
	if err != nil {
		t.Fatalf("Parse CREATE failed: %v", err)
	}
	if _, _, err := eng.Execute(stmt); err != nil {
		t.Fatalf("Execute CREATE failed: %v", err)
	}

	inserts := []string{
		"INSERT INTO users VALUES (1, 'Ada', 30);",
		"INSERT INTO users VALUES (2, 'Bea', 18);",
		"INSERT INTO users VALUES (3, 'Cara', 21);",
		"INSERT INTO users VALUES (4, 'Drew', 16);",
		"INSERT INTO users VALUES (5, 'Eli', 22);",
	}
	for _, q := range inserts {
		stmt, err := sql.Parse(q)
		if err != nil {
			t.Fatalf("Parse INSERT failed for %q: %v", q, err)
		}
		if _, _, err := eng.Execute(stmt); err != nil {
			t.Fatalf("Execute INSERT failed for %q: %v", q, err)
		}
	}

	// Projection + WHERE with ">" + ORDER BY + LIMIT
	selectSQL := "SELECT name, age FROM users WHERE age > 18 ORDER BY age LIMIT 3;"
	selStmt, err := sql.Parse(selectSQL)
	if err != nil {
		t.Fatalf("Parse SELECT failed: %v", err)
	}

	cols, rows, err := eng.Execute(selStmt)
	if err != nil {
		t.Fatalf("Execute SELECT failed: %v", err)
	}

	if len(cols) != 2 || cols[0] != "name" || cols[1] != "age" {
		t.Fatalf("unexpected projection: %#v", cols)
	}

	if len(rows) != 3 {
		t.Fatalf("expected 3 rows after LIMIT, got %d", len(rows))
	}

	gotNames := []string{rows[0][0].S, rows[1][0].S, rows[2][0].S}
	want := []string{"Cara", "Eli", "Ada"} // ages 21, 22, 30 (ordered asc)
	for i := range want {
		if gotNames[i] != want[i] {
			t.Fatalf("unexpected order at %d: got %q want %q (names=%v)", i, gotNames[i], want[i], gotNames)
		}
	}

	// WHERE with equality
	eqSQL := "SELECT id FROM users WHERE age = 18;"
	eqStmt, err := sql.Parse(eqSQL)
	if err != nil {
		t.Fatalf("Parse SELECT (=) failed: %v", err)
	}
	cols, rows, err = eng.Execute(eqStmt)
	if err != nil {
		t.Fatalf("Execute SELECT (=) failed: %v", err)
	}

	if len(rows) != 1 || rows[0][0].I64 != 2 {
		t.Fatalf("unexpected equality results: cols=%v rows=%v", cols, rows)
	}

	// WHERE with "<" and ORDER BY to keep determinism
	ltSQL := "SELECT name FROM users WHERE age < 18 ORDER BY name;"
	ltStmt, err := sql.Parse(ltSQL)
	if err != nil {
		t.Fatalf("Parse SELECT (<) failed: %v", err)
	}
	cols, rows, err = eng.Execute(ltStmt)
	if err != nil {
		t.Fatalf("Execute SELECT (<) failed: %v", err)
	}

	if len(cols) != 1 || cols[0] != "name" {
		t.Fatalf("unexpected projection for < query: %v", cols)
	}
	if len(rows) != 1 || rows[0][0].S != "Drew" {
		t.Fatalf("unexpected < query results: rows=%v", rows)
	}
}
