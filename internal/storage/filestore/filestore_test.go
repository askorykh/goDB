package filestore

import (
	"goDB/internal/sql"
	"path/filepath"
	"testing"
)

// Basic: create table, verify file exists, read schema.
func TestFilestore_CreateTableAndSchema(t *testing.T) {
	dir := t.TempDir()

	fs, err := New(dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	cols := []sql.Column{
		{Name: "id", Type: sql.TypeInt},
		{Name: "name", Type: sql.TypeString},
	}

	if err := fs.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// ListTables
	tables, err := fs.ListTables()
	if err != nil {
		t.Fatalf("ListTables failed: %v", err)
	}
	if len(tables) != 1 || tables[0] != "users" {
		t.Fatalf("unexpected tables: %v", tables)
	}

	// Schema
	schema, err := fs.TableSchema("users")
	if err != nil {
		t.Fatalf("TableSchema failed: %v", err)
	}
	if len(schema) != 2 || schema[0].Name != "id" || schema[1].Name != "name" {
		t.Fatalf("unexpected schema: %v", schema)
	}

	// And file must exist
	path := filepath.Join(dir, "users.godb")
	if _, err := filepath.Glob(path); err != nil {
		t.Fatalf("file not created: %v", err)
	}
}

// Insert → Commit → Re-open → Scan
func TestFilestore_InsertAndScan(t *testing.T) {
	dir := t.TempDir()

	fs, err := New(dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	cols := []sql.Column{
		{Name: "id", Type: sql.TypeInt},
		{Name: "name", Type: sql.TypeString},
	}

	if err := fs.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert inside tx
	tx, err := fs.Begin(false)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	row := sql.Row{
		{Type: sql.TypeInt, I64: 1},
		{Type: sql.TypeString, S: "Alice"},
	}

	if err := tx.Insert("users", row); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if err := fs.Commit(tx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// New tx (read-only)
	tx2, err := fs.Begin(true)
	if err != nil {
		t.Fatalf("Begin2 failed: %v", err)
	}

	names, rows, err := tx2.Scan("users")
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if len(names) != 2 || names[0] != "id" || names[1] != "name" {
		t.Fatalf("unexpected names: %v", names)
	}

	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}

	if rows[0][0].I64 != 1 || rows[0][1].S != "Alice" {
		t.Fatalf("unexpected rows: %v", rows)
	}
}

// Test ReplaceAll
func TestFilestore_ReplaceAll(t *testing.T) {
	dir := t.TempDir()

	fs, err := New(dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	cols := []sql.Column{
		{Name: "id", Type: sql.TypeInt},
		{Name: "active", Type: sql.TypeBool},
	}

	if err := fs.CreateTable("flags", cols); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert initial data
	tx, _ := fs.Begin(false)
	_ = tx.Insert("flags", sql.Row{{Type: sql.TypeInt, I64: 1}, {Type: sql.TypeBool, B: true}})
	_ = tx.Insert("flags", sql.Row{{Type: sql.TypeInt, I64: 2}, {Type: sql.TypeBool, B: false}})
	_ = fs.Commit(tx)

	// Replace all
	tx2, _ := fs.Begin(false)
	newRows := []sql.Row{
		{{Type: sql.TypeInt, I64: 99}, {Type: sql.TypeBool, B: false}},
	}
	if err := tx2.ReplaceAll("flags", newRows); err != nil {
		t.Fatalf("ReplaceAll failed: %v", err)
	}
	_ = fs.Commit(tx2)

	// Read back
	tx3, _ := fs.Begin(true)
	_, rows, err := tx3.Scan("flags")
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(rows) != 1 || rows[0][0].I64 != 99 {
		t.Fatalf("unexpected rows: %v", rows)
	}
}

// Rollback does NOT undo writes (documented)
func TestFilestore_Rollback_NoUndo(t *testing.T) {
	dir := t.TempDir()
	fs, err := New(dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	cols := []sql.Column{
		{Name: "id", Type: sql.TypeInt},
	}

	_ = fs.CreateTable("t", cols)

	tx, _ := fs.Begin(false)
	_ = tx.Insert("t", sql.Row{{Type: sql.TypeInt, I64: 1}})
	_ = fs.Rollback(tx) // does NOT undo writes

	// Scan should still see row
	tx2, _ := fs.Begin(true)
	_, rows, _ := tx2.Scan("t")
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}
