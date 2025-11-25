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
