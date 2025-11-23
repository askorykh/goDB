package memstore

import (
	"goDB/internal/sql"
	"testing"
)

// TestMemstoreCreateInsertScan verifies that we can create a table,
// insert rows, and read them back with Scan.
func TestMemstoreCreateInsertScan(t *testing.T) {
	store := New() // this returns storage.Engine, implemented by *memEngine

	// 1. Create table "users"
	err := store.CreateTable("users", []sql.Column{
		{Name: "id", Type: sql.TypeInt},
		{Name: "name", Type: sql.TypeString},
		{Name: "active", Type: sql.TypeBool},
	})

	// 2. Begin a read-write transaction
	tx, err := store.Begin(false /* readOnly */)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// 3. Insert two rows
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

	if err := tx.Insert("users", row1); err != nil {
		t.Fatalf("Insert row1 failed: %v", err)
	}
	if err := tx.Insert("users", row2); err != nil {
		t.Fatalf("Insert row2 failed: %v", err)
	}

	// 4. Scan the table
	cols, rows, err := tx.Scan("users")
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// (Commit doesn't do anything in memstore right now, but call it anyway.)
	if err := store.Commit(tx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// --- Assertions ---

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

	// Check row count
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Helper to assert a single row
	checkRow := func(row sql.Row, id int64, name string, active bool) {
		if len(row) != 3 {
			t.Fatalf("expected 3 values in row, got %d", len(row))
		}

		// id
		if row[0].Type != sql.TypeInt || row[0].I64 != id {
			t.Fatalf("id: expected %d, got (type=%v, value=%d)", id, row[0].Type, row[0].I64)
		}

		// name
		if row[1].Type != sql.TypeString || row[1].S != name {
			t.Fatalf("name: expected %q, got (type=%v, value=%q)", name, row[1].Type, row[1].S)
		}

		// active
		if row[2].Type != sql.TypeBool || row[2].B != active {
			t.Fatalf("active: expected %v, got (type=%v, value=%v)", active, row[2].Type, row[2].B)
		}
	}

	checkRow(rows[0], 1, "Alice", true)
	checkRow(rows[1], 2, "Bob", false)
}
