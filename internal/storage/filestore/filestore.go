package filestore

import (
	"errors"
	"fmt"
	"goDB/internal/sql"
	"goDB/internal/storage"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// FileEngine is a simple on-disk storage engine.
// It stores one file per table in the given directory.
//
// Layout:
//
//	[header][rows...]
//
// Header:
//
//	magic:     5 bytes "GODB1"
//	numCols:   uint16
//	per column:
//	  nameLen: uint16
//	  name:    nameLen bytes (UTF-8)
//	  type:    uint8 (matches sql.DataType)
//
// Rows:
//
//	For each row:
//	  For each column:
//	    type: uint8 (sql.DataType, allows NULL vs non-NULL)
//	    payload (depends on type):
//	      INT:    int64 (little endian)
//	      FLOAT:  float64 (little endian)
//	      STRING: uint32 length + bytes
//	      BOOL:   1 byte (0 or 1)
//	      NULL:   no payload
type FileEngine struct {
	dir string
	wal *walLogger

	// tx ID generator (for write tx only)
	mu       sync.Mutex
	nextTxID uint64
}

// ListTables returns all *.godb files in the storage directory.
func (e *FileEngine) ListTables() ([]string, error) {
	entries, err := os.ReadDir(e.dir)
	if err != nil {
		return nil, fmt.Errorf("filestore: list tables: %w", err)
	}

	var tables []string
	for _, ent := range entries {
		name := ent.Name()
		if strings.HasSuffix(name, ".godb") {
			// table name = filename without extension
			t := strings.TrimSuffix(name, ".godb")
			tables = append(tables, t)
		}
	}
	return tables, nil
}

// TableSchema reads the schema header of the given table.
func (e *FileEngine) TableSchema(name string) ([]sql.Column, error) {
	path := e.tablePath(name)

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("filestore: open table for schema: %w", err)
	}
	defer f.Close()

	cols, err := readHeader(f)
	if err != nil {
		return nil, fmt.Errorf("filestore: read header in schema: %w", err)
	}

	return cols, nil
}

// New creates a new FileEngine storing all tables in dir.
func New(dir string) (*FileEngine, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("filestore: create dir: %w", err)
	}

	w, err := newWAL(dir)
	if err != nil {
		return nil, fmt.Errorf("filestore: init WAL: %w", err)
	}

	e := &FileEngine{
		dir:      dir,
		wal:      w,
		nextTxID: 1,
	}

	// Recover database state from WAL on startup.
	if err := e.recoverFromWAL(); err != nil {
		return nil, fmt.Errorf("filestore: recovery failed: %w", err)
	}

	return e, nil
}

func (e *FileEngine) tablePath(name string) string {
	// very simple mapping: "<dir>/<name>.godb"
	// (you may want to sanitize name further later)
	return filepath.Join(e.dir, name+".godb")
}

// CreateTable creates a new table file with the given schema.
func (e *FileEngine) CreateTable(name string, cols []sql.Column) error {
	path := e.tablePath(name)

	if _, err := os.Stat(path); err == nil {
		return fmt.Errorf("filestore: table %q already exists", name)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("filestore: check existing table: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("filestore: create table file: %w", err)
	}
	defer f.Close()

	if err := writeHeader(f, cols); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return fmt.Errorf("filestore: write header: %w", err)
	}

	return nil
}

// Begin starts a new (very simple) transaction.
// NOTE: For now this does NOT support rollback-on-disk; it is mainly
// used to group operations logically. Real WAL/rollback can be added later.
func (e *FileEngine) Begin(readOnly bool) (storage.Tx, error) {
	tx := &fileTx{
		eng:      e,
		readOnly: readOnly,
		closed:   false,
		id:       0,
	}

	// Only write transactions get a txID and BEGIN record.
	if !readOnly {
		e.mu.Lock()
		txID := e.nextTxID
		e.nextTxID++
		e.mu.Unlock()

		tx.id = txID

		if err := e.wal.appendBegin(txID); err != nil {
			return nil, fmt.Errorf("filestore: WAL BEGIN: %w", err)
		}
	}

	return tx, nil
}

func (e *FileEngine) Commit(tx storage.Tx) error {
	ft, err := e.validateTx(tx)
	if err != nil {
		return err
	}

	// For write transactions, append COMMIT and sync WAL.
	if !ft.readOnly && ft.id != 0 {
		if err := e.wal.appendCommit(ft.id); err != nil {
			return fmt.Errorf("filestore: WAL COMMIT: %w", err)
		}
		if err := e.wal.Sync(); err != nil {
			return fmt.Errorf("filestore: WAL sync on commit: %w", err)
		}
	}

	ft.closed = true
	return nil
}

func (e *FileEngine) Rollback(tx storage.Tx) error {
	ft, err := e.validateTx(tx)
	if err != nil {
		return err
	}

	// Still no actual undo on disk, but we log ROLLBACK for future recovery logic.
	if !ft.readOnly && ft.id != 0 {
		if err := e.wal.appendRollback(ft.id); err != nil {
			return fmt.Errorf("filestore: WAL ROLLBACK: %w", err)
		}
		if err := e.wal.Sync(); err != nil {
			return fmt.Errorf("filestore: WAL sync on rollback: %w", err)
		}
	}

	ft.closed = true
	return nil
}
