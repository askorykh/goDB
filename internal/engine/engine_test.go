package engine

import (
	"goDB/internal/sql"
	"goDB/internal/storage/memstore"
	"testing"
)

// TestEngineCreateInsertSelectAll checks the engine API end-to-end
// using the in-memory storage engine.
func TestEngineCreateInsertSelectAll(t *testing.T) {
	// 1. Set up engine with memstore.
	store := memstore.New()
	eng := New(store)

	if err := eng.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// 2. Create table "users".
	if err := eng.CreateTable("users", []sql.Column{
		{Name: "id", Type: sql.TypeInt},
		{Name: "name", Type: sql.TypeString},
		{Name: "active", Type: sql.TypeBool},
	}); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// 3. Insert two rows via engine API.
	row1 := sql.Row{
		{Type: sql.TypeInt, I64: 1},
		{Type: sql.TypeString, S: "Alice"},
		{Type: sql.TypeBool, B: true},
	}
	row2 := sql.Row{
		{Type: sql.TypeInt, I64: 2},
		{Type: sql.TypeString, S: "Bob"},
		{Type: sql.TypeBool, B: false},
	}

	if err := eng.InsertRow("users", row1); err != nil {
		t.Fatalf("InsertRow row1 failed: %v", err)
	}
	if err := eng.InsertRow("users", row2); err != nil {
		t.Fatalf("InsertRow row2 failed: %v", err)
	}

	// 4. SelectAll and assert results.
	cols, rows, err := eng.SelectAll("users")
	if err != nil {
		t.Fatalf("SelectAll failed: %v", err)
	}

	// Check columns
	expectedCols := []string{"id", "name", "active"}
	if len(cols) != len(expectedCols) {
		t.Fatalf("expected %d columns, got %d", len(expectedCols), len(cols))
	}
	for i, want := range expectedCols {
		if cols[i] != want {
			t.Fatalf("column %d: expected %q, got %q", i, want, cols[i])
		}
	}

	// Check rows
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	checkRow := func(row sql.Row, id int64, name string, active bool) {
		if len(row) != 3 {
			t.Fatalf("expected 3 values in row, got %d", len(row))
		}
		if row[0].Type != sql.TypeInt || row[0].I64 != id {
			t.Fatalf("id: expected %d, got (type=%v, value=%d)", id, row[0].Type, row[0].I64)
		}
		if row[1].Type != sql.TypeString || row[1].S != name {
			t.Fatalf("name: expected %q, got (type=%v, value=%q)", name, row[1].Type, row[1].S)
		}
		if row[2].Type != sql.TypeBool || row[2].B != active {
			t.Fatalf("active: expected %v, got (type=%v, value=%v)", active, row[2].Type, row[2].B)
		}
	}

	checkRow(rows[0], 1, "Alice", true)
	checkRow(rows[1], 2, "Bob", false)
}

func TestEngineExecute_CreateTableAndUseIt(t *testing.T) {
	// 1. Set up engine with memstore.
	store := memstore.New()
	eng := New(store)

	if err := eng.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// 2. Parse a CREATE TABLE statement.
	query := "CREATE TABLE users (id INT, name STRING, active BOOL);"
	stmt, err := sql.Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Sanity check: make sure parser produced the right type.
	if _, ok := stmt.(*sql.CreateTableStmt); !ok {
		t.Fatalf("expected *CreateTableStmt, got %T", stmt)
	}

	// 3. Execute the statement via the engine.
	if _, _, err := eng.Execute(stmt); err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	// 4. Insert and select to prove the table is correctly created.
	row1 := sql.Row{
		{Type: sql.TypeInt, I64: 1},
		{Type: sql.TypeString, S: "Alice"},
		{Type: sql.TypeBool, B: true},
	}
	if err := eng.InsertRow("users", row1); err != nil {
		t.Fatalf("InsertRow row1 failed: %v", err)
	}

	row2 := sql.Row{
		{Type: sql.TypeInt, I64: 2},
		{Type: sql.TypeString, S: "Bob"},
		{Type: sql.TypeBool, B: false},
	}
	if err := eng.InsertRow("users", row2); err != nil {
		t.Fatalf("InsertRow row2 failed: %v", err)
	}

	cols, rows, err := eng.SelectAll("users")
	if err != nil {
		t.Fatalf("SelectAll failed: %v", err)
	}

	expectedCols := []string{"id", "name", "active"}
	if len(cols) != len(expectedCols) {
		t.Fatalf("expected %d columns, got %d", len(expectedCols), len(cols))
	}
	for i, want := range expectedCols {
		if cols[i] != want {
			t.Fatalf("column %d: expected %q, got %q", i, want, cols[i])
		}
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}
func TestEngineExecute_InsertViaSQL(t *testing.T) {
	store := memstore.New()
	eng := New(store)

	if err := eng.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// 1. CREATE TABLE via SQL
	createSQL := "CREATE TABLE users (id INT, name STRING, active BOOL);"
	createStmt, err := sql.Parse(createSQL)
	if err != nil {
		t.Fatalf("Parse CREATE failed: %v", err)
	}
	if _, _, err := eng.Execute(createStmt); err != nil {
		t.Fatalf("Execute CREATE failed: %v", err)
	}

	// 2. INSERT rows via SQL
	insert1 := "INSERT INTO users VALUES (1, 'Alice', true);"
	insert2 := "INSERT INTO users VALUES (2, 'Bob', false);"

	for _, q := range []string{insert1, insert2} {
		stmt, err := sql.Parse(q)
		if err != nil {
			t.Fatalf("Parse INSERT failed for %q: %v", q, err)
		}
		if _, _, err := eng.Execute(stmt); err != nil {
			t.Fatalf("Execute INSERT failed for %q: %v", q, err)
		}
	}

	// 3. SELECT via engine API
	cols, rows, err := eng.SelectAll("users")
	if err != nil {
		t.Fatalf("SelectAll failed: %v", err)
	}

	expectedCols := []string{"id", "name", "active"}
	if len(cols) != len(expectedCols) {
		t.Fatalf("expected %d columns, got %d", len(expectedCols), len(cols))
	}
	for i, want := range expectedCols {
		if cols[i] != want {
			t.Fatalf("column %d: expected %q, got %q", i, want, cols[i])
		}
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}
func TestEngineExecute_SelectViaSQL(t *testing.T) {
	store := memstore.New()
	eng := New(store)

	if err := eng.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// 1. CREATE TABLE via SQL
	createSQL := "CREATE TABLE users (id INT, name STRING, active BOOL);"
	createStmt, err := sql.Parse(createSQL)
	if err != nil {
		t.Fatalf("Parse CREATE failed: %v", err)
	}
	if _, _, err := eng.Execute(createStmt); err != nil {
		t.Fatalf("Execute CREATE failed: %v", err)
	}

	// 2. INSERT rows via SQL
	insert1 := "INSERT INTO users VALUES (1, 'Alice', true);"
	insert2 := "INSERT INTO users VALUES (2, 'Bob', false);"

	for _, q := range []string{insert1, insert2} {
		stmt, err := sql.Parse(q)
		if err != nil {
			t.Fatalf("Parse INSERT failed for %q: %v", q, err)
		}
		if _, _, err := eng.Execute(stmt); err != nil {
			t.Fatalf("Execute INSERT failed for %q: %v", q, err)
		}
	}

	// 3. SELECT via SQL using Execute (not SelectAll directly)
	selectSQL := "SELECT * FROM users;"
	selectStmt, err := sql.Parse(selectSQL)
	if err != nil {
		t.Fatalf("Parse SELECT failed: %v", err)
	}

	cols, rows, err := eng.Execute(selectStmt)
	if err != nil {
		t.Fatalf("Execute SELECT failed: %v", err)
	}

	expectedCols := []string{"id", "name", "active"}
	if len(cols) != len(expectedCols) {
		t.Fatalf("expected %d columns, got %d", len(expectedCols), len(cols))
	}
	for i, want := range expectedCols {
		if cols[i] != want {
			t.Fatalf("column %d: expected %q, got %q", i, want, cols[i])
		}
	}

	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
}
func TestEngineExecute_SelectWithWhere(t *testing.T) {
	store := memstore.New()
	eng := New(store)

	if err := eng.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// CREATE TABLE via SQL
	createSQL := "CREATE TABLE users (id INT, name STRING, active BOOL);"
	createStmt, err := sql.Parse(createSQL)
	if err != nil {
		t.Fatalf("Parse CREATE failed: %v", err)
	}
	if _, _, err := eng.Execute(createStmt); err != nil {
		t.Fatalf("Execute CREATE failed: %v", err)
	}

	// INSERT rows via SQL
	queries := []string{
		"INSERT INTO users VALUES (1, 'Alice', true);",
		"INSERT INTO users VALUES (2, 'Bob', false);",
		"INSERT INTO users VALUES (3, 'Alice Smith', true);",
	}
	for _, q := range queries {
		stmt, err := sql.Parse(q)
		if err != nil {
			t.Fatalf("Parse INSERT failed for %q: %v", q, err)
		}
		if _, _, err := eng.Execute(stmt); err != nil {
			t.Fatalf("Execute INSERT failed for %q: %v", q, err)
		}
	}

	// SELECT * FROM users WHERE active = true;
	selectSQL := "SELECT * FROM users WHERE active = true;"
	selectStmt, err := sql.Parse(selectSQL)
	if err != nil {
		t.Fatalf("Parse SELECT failed: %v", err)
	}

	cols, rows, err := eng.Execute(selectStmt)
	if err != nil {
		t.Fatalf("Execute SELECT failed: %v", err)
	}

	// Expect 2 rows: Alice and Alice Smith
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows with active = true, got %d", len(rows))
	}

	// Simple sanity: check column headers still correct
	expectedCols := []string{"id", "name", "active"}
	if len(cols) != len(expectedCols) {
		t.Fatalf("expected %d columns, got %d", len(expectedCols), len(cols))
	}
	for i, want := range expectedCols {
		if cols[i] != want {
			t.Fatalf("column %d: expected %q, got %q", i, want, cols[i])
		}
	}
}
func TestEngineExecute_SelectColumnList(t *testing.T) {
	store := memstore.New()
	eng := New(store)

	if err := eng.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// CREATE TABLE + INSERT via SQL.
	createSQL := "CREATE TABLE users (id INT, name STRING, active BOOL);"
	createStmt, err := sql.Parse(createSQL)
	if err != nil {
		t.Fatalf("Parse CREATE failed: %v", err)
	}
	if _, _, err := eng.Execute(createStmt); err != nil {
		t.Fatalf("Execute CREATE failed: %v", err)
	}

	insert := []string{
		"INSERT INTO users VALUES (1, 'Alice', true);",
		"INSERT INTO users VALUES (2, 'Bob', false);",
	}
	for _, q := range insert {
		stmt, err := sql.Parse(q)
		if err != nil {
			t.Fatalf("Parse INSERT failed for %q: %v", q, err)
		}
		if _, _, err := eng.Execute(stmt); err != nil {
			t.Fatalf("Execute INSERT failed for %q: %v", q, err)
		}
	}

	// SELECT id, name FROM users WHERE active = true;
	selectSQL := "SELECT id, name FROM users WHERE active = true;"
	stmt, err := sql.Parse(selectSQL)
	if err != nil {
		t.Fatalf("Parse SELECT failed: %v", err)
	}

	cols, rows, err := eng.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute SELECT failed: %v", err)
	}

	// We projected to 2 columns only.
	if len(cols) != 2 || cols[0] != "id" || cols[1] != "name" {
		t.Fatalf("unexpected projected columns: %#v", cols)
	}

	// Only Alice is active.
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if len(rows[0]) != 2 {
		t.Fatalf("expected 2 values in row, got %d", len(rows[0]))
	}
}
