package filestore

import (
	"encoding/binary"
	"goDB/internal/sql"
	"io"
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
func TestFilestore_WAL_BeginCommit(t *testing.T) {
	dir := t.TempDir()

	fs, err := New(dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	cols := []sql.Column{{Name: "id", Type: sql.TypeInt}}
	if err := fs.CreateTable("t", cols); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	tx, _ := fs.Begin(false)
	_ = fs.Commit(tx)

	walPath := filepath.Join(dir, "wal.log")
	f, err := os.Open(walPath)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer f.Close()

	// skip magic
	if _, err := f.Seek(int64(len("GODBWAL2")), io.SeekStart); err != nil {
		t.Fatalf("seek: %v", err)
	}

	var recType uint8
	if err := binary.Read(f, binary.LittleEndian, &recType); err != nil {
		t.Fatalf("read recType: %v", err)
	}
	if recType != 1 { // BEGIN
		t.Fatalf("expected first record to be BEGIN (1), got %d", recType)
	}
}
