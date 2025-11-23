package filestore

import (
	"errors"
	"goDB/internal/sql"
	"os"
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
	tx, err := fs.Begin(false)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := tx.Insert("flags", sql.Row{{Type: sql.TypeInt, I64: 1}, {Type: sql.TypeBool, B: true}}); err != nil {
		t.Fatalf("Insert1 failed: %v", err)
	}
	if err := tx.Insert("flags", sql.Row{{Type: sql.TypeInt, I64: 2}, {Type: sql.TypeBool, B: false}}); err != nil {
		t.Fatalf("Insert2 failed: %v", err)
	}
	if err := fs.Commit(tx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Replace all
	tx2, err := fs.Begin(false)
	if err != nil {
		t.Fatalf("Begin2 failed: %v", err)
	}
	newRows := []sql.Row{
		{{Type: sql.TypeInt, I64: 99}, {Type: sql.TypeBool, B: false}},
	}
	if err := tx2.ReplaceAll("flags", newRows); err != nil {
		t.Fatalf("ReplaceAll failed: %v", err)
	}
	if err := fs.Commit(tx2); err != nil {
		t.Fatalf("Commit2 failed: %v", err)
	}

	// Read back
	tx3, err := fs.Begin(true)
	if err != nil {
		t.Fatalf("Begin3 failed: %v", err)
	}
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

	if err := fs.CreateTable("t", cols); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	tx, err := fs.Begin(false)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := tx.Insert("t", sql.Row{{Type: sql.TypeInt, I64: 1}}); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}
	if err := fs.Rollback(tx); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	} // does NOT undo writes

	// Scan should still see row
	tx2, err := fs.Begin(true)
	if err != nil {
		t.Fatalf("Begin2 failed: %v", err)
	}
	_, rows, err := tx2.Scan("t")
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
}

func TestFilestore_CommitRollbackValidation(t *testing.T) {
	dir := t.TempDir()
	fs, err := New(dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	if err := fs.Commit(nil); err == nil {
		t.Fatalf("expected error committing nil tx")
	}
	if err := fs.Rollback(nil); err == nil {
		t.Fatalf("expected error rolling back nil tx")
	}

	// Commit marks transaction closed.
	tx, err := fs.Begin(false)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}
	if err := fs.Commit(tx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}
	if err := fs.Commit(tx); err == nil {
		t.Fatalf("expected commit on closed tx to fail")
	}

	// Rollback also closes the transaction.
	tx2, err := fs.Begin(false)
	if err != nil {
		t.Fatalf("Begin2 failed: %v", err)
	}
	if err := fs.Rollback(tx2); err != nil {
		t.Fatalf("Rollback failed: %v", err)
	}
	if err := fs.Rollback(tx2); err == nil {
		t.Fatalf("expected rollback on closed tx to fail")
	}
}

func TestFilestore_CreateTableTooManyColumns(t *testing.T) {
	dir := t.TempDir()
	fs, err := New(dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	cols := make([]sql.Column, 0x10000)
	for i := range cols {
		cols[i] = sql.Column{Name: "c", Type: sql.TypeInt}
	}

	err = fs.CreateTable("big", cols)
	if err == nil {
		t.Fatalf("expected error for too many columns")
	}

	// Ensure the file is not left behind when table creation fails.
	path := filepath.Join(dir, "big.godb")
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("table file should not remain after failure")
	}
}
