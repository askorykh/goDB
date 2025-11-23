package filestore

import (
	"fmt"
	"goDB/internal/sql"
	"io"
	"os"
)

// fileTx implements storage.Tx for FileEngine.
type fileTx struct {
	eng      *FileEngine
	readOnly bool
	closed   bool
	id       uint64 // 0 = no WAL tracking (read-only or not started)
}

// Insert appends a row to the table file.
func (tx *fileTx) Insert(tableName string, row sql.Row) error {
	if tx.closed {
		return fmt.Errorf("filestore: tx is closed")
	}
	if tx.readOnly {
		return fmt.Errorf("filestore: cannot insert in read-only transaction")
	}

	// 1) WAL: log the insert first
	if !tx.readOnly && tx.id != 0 {
		if err := tx.eng.wal.appendInsert(tx.id, tableName, row); err != nil {
			return fmt.Errorf("filestore: WAL appendInsert: %w", err)
		}
	}

	// 2) Then write to table file
	path := tx.eng.tablePath(tableName)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("filestore: open table for insert: %w", err)
	}
	defer f.Close()

	cols, err := readHeader(f)
	if err != nil {
		return fmt.Errorf("filestore: read header in insert: %w", err)
	}

	if len(row) != len(cols) {
		return fmt.Errorf("filestore: row has %d values, expected %d", len(row), len(cols))
	}

	// seek to end and append row
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("filestore: seek end: %w", err)
	}

	if err := writeRow(f, row); err != nil {
		return fmt.Errorf("filestore: write row: %w", err)
	}

	return nil
}

// Scan reads all rows from the table file.
func (tx *fileTx) Scan(tableName string) ([]string, []sql.Row, error) {
	if tx.closed {
		return nil, nil, fmt.Errorf("filestore: tx is closed")
	}

	path := tx.eng.tablePath(tableName)
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, fmt.Errorf("filestore: open table for scan: %w", err)
	}
	defer f.Close()

	cols, err := readHeader(f)
	if err != nil {
		return nil, nil, fmt.Errorf("filestore: read header in scan: %w", err)
	}

	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}

	var rows []sql.Row
	for {
		row, err := readRow(f, len(cols))
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("filestore: read row: %w", err)
		}
		rows = append(rows, row)
	}

	return colNames, rows, nil
}

// ReplaceAll truncates the table file and rewrites header + rows.
func (tx *fileTx) ReplaceAll(tableName string, rows []sql.Row) error {
	if tx.closed {
		return fmt.Errorf("filestore: tx is closed")
	}
	if tx.readOnly {
		return fmt.Errorf("filestore: cannot replace in read-only transaction")
	}

	//  WAL: log replaceAll
	if !tx.readOnly && tx.id != 0 {
		if err := tx.eng.wal.appendReplaceAll(tx.id, tableName, rows); err != nil {
			return fmt.Errorf("filestore: WAL appendReplaceAll: %w", err)
		}
	}

	path := tx.eng.tablePath(tableName)

	// We need the schema from the existing file
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("filestore: open table for replace: %w", err)
	}
	defer f.Close()

	cols, err := readHeader(f)
	if err != nil {
		return fmt.Errorf("filestore: read header in replace: %w", err)
	}

	// truncate file and rewrite header + rows
	if err := f.Truncate(0); err != nil {
		return fmt.Errorf("filestore: truncate: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("filestore: seek start: %w", err)
	}

	if err := writeHeader(f, cols); err != nil {
		return fmt.Errorf("filestore: write header in replace: %w", err)
	}

	for _, r := range rows {
		if len(r) != len(cols) {
			return fmt.Errorf("filestore: replace row length mismatch: got %d, expected %d",
				len(r), len(cols))
		}
		if err := writeRow(f, r); err != nil {
			return fmt.Errorf("filestore: write row in replace: %w", err)
		}
	}

	return nil
}
