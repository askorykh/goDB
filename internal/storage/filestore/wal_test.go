package filestore

import (
	"goDB/internal/sql"
	"os"
	"path/filepath"
	"testing"
)

func TestFilestore_WAL_IsWritten(t *testing.T) {
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

	row := sql.Row{{Type: sql.TypeInt, I64: 42}}
	if err := tx.Insert("t", row); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if err := fs.Commit(tx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Check WAL file exists and is non-empty
	walPath := filepath.Join(dir, "wal.log")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("wal.log not found: %v", err)
	}
	if info.Size() <= int64(len("GODBWAL1")) {
		t.Fatalf("wal.log too small, no records? size=%d", info.Size())
	}
}
