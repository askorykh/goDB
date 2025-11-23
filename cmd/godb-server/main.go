package main

import (
	"fmt"
	"goDB/internal/engine"
	"goDB/internal/sql"
	"goDB/internal/storage/memstore"
	"strings"
)

func main() {
	fmt.Println("GoDB server starting (v0)…")

	// Create the in-memory storage engine.
	store := memstore.New()

	// Create the database engine
	eng := engine.New(store)

	// Start the engine
	if err := eng.Start(); err != nil {
		fmt.Println("ERROR:", err)
		return
	}

	fmt.Println("Engine started successfully (using in-memory storage).")

	// Define a simple "users" table with 3 columns.
	if err := eng.CreateTable("users", []sql.Column{
		{Name: "id", Type: sql.TypeInt},
		{Name: "name", Type: sql.TypeString},
		{Name: "active", Type: sql.TypeBool},
	}); err != nil {
		fmt.Println("CreateTable ERROR:", err)
		return
	}
	fmt.Println("Table 'users' created.")

	// Create a row: (1, "Alice", true)
	row1 := sql.Row{
		{Type: sql.TypeInt, I64: 1},
		{Type: sql.TypeString, S: "Alice"},
		{Type: sql.TypeBool, B: true},
	}

	// Insert the row.
	if err := eng.InsertRow("users", row1); err != nil {
		fmt.Println("InsertRow ERROR:", err)
		return
	}
	fmt.Println("Inserted row 1 into 'users'.")

	// Create another row: (2, "Bob", false)
	row2 := sql.Row{
		{Type: sql.TypeInt, I64: 2},
		{Type: sql.TypeString, S: "Bob"},
		{Type: sql.TypeBool, B: false},
	}

	if err := eng.InsertRow("users", row2); err != nil {
		fmt.Println("InsertRow ERROR:", err)
		return
	}
	fmt.Println("Inserted row 2 into 'users'.")

	// --- SELECT * FROM users (using SelectAll) ---

	fmt.Println("\nSelecting all from 'users':")

	cols, rows, err := eng.SelectAll("users")
	if err != nil {
		fmt.Println("SelectAll ERROR:", err)
		return
	}

	// Print header
	fmt.Println(strings.Join(cols, " | "))

	// Print each row
	for _, row := range rows {
		var parts []string
		for _, v := range row {
			parts = append(parts, formatValue(v))
		}
		fmt.Println(strings.Join(parts, " | "))
	}

}

// formatValue converts a sql.Value to a human-readable string.
func formatValue(v sql.Value) string {
	switch v.Type {
	case sql.TypeInt:
		return fmt.Sprintf("%d", v.I64)
	case sql.TypeFloat:
		return fmt.Sprintf("%f", v.F64)
	case sql.TypeString:
		return v.S
	case sql.TypeBool:
		if v.B {
			return "true"
		}
		return "false"
	default:
		return "NULL"
	}
}
