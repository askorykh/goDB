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

// Insert using a page structure
func (tx *fileTx) Insert(tableName string, row sql.Row) error {
	if tx.closed {
		return fmt.Errorf("filestore: tx is closed")
	}
	if tx.readOnly {
		return fmt.Errorf("filestore: cannot insert in read-only transaction")
	}

	// WAL: log the row first (redo-only for now)
	if !tx.readOnly && tx.id != 0 {
		if err := tx.eng.wal.appendInsert(tx.id, tableName, row); err != nil {
			return fmt.Errorf("filestore: WAL appendInsert: %w", err)
		}
	}

	path := tx.eng.tablePath(tableName)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("filestore: open table for insert: %w", err)
	}
	defer f.Close()

	// 1) Read schema + figure out header size
	cols, err := readHeader(f)
	if err != nil {
		return fmt.Errorf("filestore: read header in insert: %w", err)
	}
	if len(row) != len(cols) {
		return fmt.Errorf("filestore: row has %d values, expected %d", len(row), len(cols))
	}
	headerEnd, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("filestore: seek after header: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("filestore: stat table: %w", err)
	}
	fileSize := fi.Size()
	if fileSize < headerEnd {
		return fmt.Errorf("filestore: corrupt file, size < header")
	}

	dataBytes := fileSize - headerEnd
	var numPages uint32
	if dataBytes > 0 {
		if dataBytes%PageSize != 0 {
			return fmt.Errorf("filestore: corrupt data section (not multiple of page size)")
		}
		numPages = uint32(dataBytes / PageSize)
	} else {
		numPages = 0
	}

	rowBytes, err := encodeRowToBytes(row)
	if err != nil {
		return fmt.Errorf("filestore: encode row: %w", err)
	}

	// Helper to write a page back to disk
	writePage := func(pageID uint32, p pageBuf) error {
		offset := headerEnd + int64(pageID)*PageSize
		if _, err := f.WriteAt(p, offset); err != nil {
			return fmt.Errorf("filestore: write page %d: %w", pageID, err)
		}
		return nil
	}

	if numPages == 0 {
		// No pages yet -> allocate first page
		p := newEmptyHeapPage(0)
		if _, err := p.insertRow(rowBytes); err != nil {
			return fmt.Errorf("filestore: insert into empty page: %w", err)
		}
		return writePage(0, p)
	}

	// There are pages, try last one first
	lastID := numPages - 1
	p := make(pageBuf, PageSize)
	offset := headerEnd + int64(lastID)*PageSize
	if _, err := f.ReadAt(p, offset); err != nil {
		return fmt.Errorf("filestore: read last page: %w", err)
	}

	if _, err := p.insertRow(rowBytes); err == nil {
		// fits in last page
		return writePage(lastID, p)
	}

	// does not fit -> create new page
	newID := numPages
	p = newEmptyHeapPage(newID)
	if _, err := p.insertRow(rowBytes); err != nil {
		return fmt.Errorf("filestore: insert into new page: %w", err)
	}
	return writePage(newID, p)
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
	headerEnd, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, nil, fmt.Errorf("filestore: seek after header: %w", err)
	}

	colNames := make([]string, len(cols))
	for i, c := range cols {
		colNames[i] = c.Name
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("filestore: stat table in scan: %w", err)
	}
	fileSize := fi.Size()
	if fileSize < headerEnd {
		return nil, nil, fmt.Errorf("filestore: corrupt file, size < header")
	}
	dataBytes := fileSize - headerEnd
	if dataBytes == 0 {
		// no pages yet
		return colNames, nil, nil
	}
	if dataBytes%PageSize != 0 {
		return nil, nil, fmt.Errorf("filestore: corrupt data (not multiple of page size)")
	}
	numPages := uint32(dataBytes / PageSize)

	var rows []sql.Row
	for pageID := uint32(0); pageID < numPages; pageID++ {
		p := make(pageBuf, PageSize)
		offset := headerEnd + int64(pageID)*PageSize
		if _, err := f.ReadAt(p, offset); err != nil {
			return nil, nil, fmt.Errorf("filestore: read page %d: %w", pageID, err)
		}

		err := p.iterateRows(len(cols), func(slot uint16, r sql.Row) error {
			rows = append(rows, r)
			return nil
		})
		if err != nil {
			return nil, nil, fmt.Errorf("filestore: iterate rows in page %d: %w", pageID, err)
		}
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

	// WAL: log REPLACEALL
	if !tx.readOnly && tx.id != 0 {
		if err := tx.eng.wal.appendReplaceAll(tx.id, tableName, rows); err != nil {
			return fmt.Errorf("filestore: WAL appendReplaceAll: %w", err)
		}
	}

	path := tx.eng.tablePath(tableName)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("filestore: open table for replace: %w", err)
	}
	defer f.Close()

	// Read schema first
	cols, err := readHeader(f)
	if err != nil {
		return fmt.Errorf("filestore: read header in replace: %w", err)
	}
	if len(cols) == 0 {
		return fmt.Errorf("filestore: replace on table %q with no columns", tableName)
	}

	// Ensure row sizes match schema
	for i, r := range rows {
		if len(r) != len(cols) {
			return fmt.Errorf("filestore: replace row %d length mismatch: got %d, expected %d",
				i, len(r), len(cols))
		}
	}

	// Truncate file and rewrite header
	if err := f.Truncate(0); err != nil {
		return fmt.Errorf("filestore: truncate in replace: %w", err)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("filestore: seek start in replace: %w", err)
	}
	if err := writeHeader(f, cols); err != nil {
		return fmt.Errorf("filestore: write header in replace: %w", err)
	}
	headerEnd, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("filestore: seek after header in replace: %w", err)
	}

	// Fill pages with all rows
	pageID := uint32(0)
	p := newEmptyHeapPage(pageID)

	writePage := func(id uint32, pg pageBuf) error {
		offset := headerEnd + int64(id)*PageSize
		if _, err := f.WriteAt(pg, offset); err != nil {
			return fmt.Errorf("filestore: write page %d in replace: %w", id, err)
		}
		return nil
	}

	for _, r := range rows {
		rowBytes, err := encodeRowToBytes(r)
		if err != nil {
			return fmt.Errorf("filestore: encode row in replace: %w", err)
		}

		if _, err := p.insertRow(rowBytes); err != nil {
			// current page full -> flush and create new
			if err := writePage(pageID, p); err != nil {
				return err
			}
			pageID++
			p = newEmptyHeapPage(pageID)
			if _, err := p.insertRow(rowBytes); err != nil {
				return fmt.Errorf("filestore: insert into new page in replace: %w", err)
			}
		}
	}

	// If we inserted at least one row, flush last page.
	// If rows == 0, we don't write any page (empty table).
	if len(rows) > 0 {
		if err := writePage(pageID, p); err != nil {
			return err
		}
	}

	return nil
}
