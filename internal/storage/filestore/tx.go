package filestore

import (
	"fmt"
	"goDB/internal/index/btree"
	"goDB/internal/sql"
	"goDB/internal/storage"
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

func (tx *fileTx) DeleteWhere(tableName string, pred storage.RowPredicate) error {
	if tx.closed {
		return fmt.Errorf("filestore: tx is closed")
	}
	if tx.readOnly {
		return fmt.Errorf("filestore: cannot delete in read-only tx")
	}

	path := tx.eng.tablePath(tableName)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("filestore: open table for delete: %w", err)
	}
	defer f.Close()

	// Read header to get schema and header size.
	cols, err := readHeader(f)
	if err != nil {
		return fmt.Errorf("filestore: read header in delete: %w", err)
	}
	headerEnd, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("filestore: seek after header in delete: %w", err)
	}

	// Determine number of pages.
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("filestore: stat table in delete: %w", err)
	}
	fileSize := fi.Size()
	if fileSize < headerEnd {
		return fmt.Errorf("filestore: corrupt file, size < header")
	}
	dataBytes := fileSize - headerEnd
	if dataBytes == 0 {
		// no pages, nothing to delete
		return nil
	}
	if dataBytes%PageSize != 0 {
		return fmt.Errorf("filestore: corrupt data in delete (not multiple of page size)")
	}
	numPages := uint32(dataBytes / PageSize)

	for pageID := uint32(0); pageID < numPages; pageID++ {
		p := make(pageBuf, PageSize)
		offset := headerEnd + int64(pageID)*PageSize

		if _, err := f.ReadAt(p, offset); err != nil {
			return fmt.Errorf("filestore: read page %d in delete: %w", pageID, err)
		}

		nSlots := p.numSlots()
		for i := uint16(0); i < nSlots; i++ {
			off, length := p.getSlot(i)
			if off == 0xFFFF || length == 0 {
				// already deleted / empty
				continue
			}

			start := int(off)
			end := start + int(length)
			if end > len(p) {
				return fmt.Errorf("filestore: corrupt slot %d in delete", i)
			}

			rowBytes := p[start:end]
			row, err := readRowFromBytes(rowBytes, len(cols))
			if err != nil {
				return fmt.Errorf("filestore: read row in delete: %w", err)
			}

			match, err := pred(row)
			if err != nil {
				return err
			}
			if match {
				// WAL: log delete
				if !tx.readOnly && tx.id != 0 {
					if err := tx.eng.wal.appendDelete(tx.id, tableName, row); err != nil {
						return fmt.Errorf("filestore: WAL appendDelete: %w", err)
					}
				}
				p.deleteSlot(i)
			}
		}

		// Write modified page back to disk.
		if _, err := f.WriteAt(p, offset); err != nil {
			return fmt.Errorf("filestore: write page %d in delete: %w", pageID, err)
		}
	}

	// NOTE: currently we do NOT log per-row deletes in WAL, so crash recovery
	// may not restore these deletes. We’ll address WAL integration later.
	return nil
}

func (tx *fileTx) UpdateWhere(tableName string, pred storage.RowPredicate, updater storage.RowUpdater) error {
	if tx.closed {
		return fmt.Errorf("filestore: tx is closed")
	}
	if tx.readOnly {
		return fmt.Errorf("filestore: cannot update in read-only tx")
	}

	path := tx.eng.tablePath(tableName)
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("filestore: open table for update: %w", err)
	}
	defer f.Close()

	// Read table schema from header
	cols, err := readHeader(f)
	if err != nil {
		return fmt.Errorf("filestore: read header in update: %w", err)
	}
	headerEnd, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("filestore: seek after header in update: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("filestore: stat table in update: %w", err)
	}
	fileSize := fi.Size()
	if fileSize < headerEnd {
		return fmt.Errorf("filestore: corrupt file, size < header")
	}
	dataBytes := fileSize - headerEnd
	if dataBytes == 0 {
		// no pages -> nothing to update
		return nil
	}
	if dataBytes%PageSize != 0 {
		return fmt.Errorf("filestore: corrupt data in update (not multiple of page size)")
	}
	numPages := uint32(dataBytes / PageSize)

	var extraRows []sql.Row // updated rows that no longer fit in place

	for pageID := uint32(0); pageID < numPages; pageID++ {
		p := make(pageBuf, PageSize)
		offset := headerEnd + int64(pageID)*PageSize

		if _, err := f.ReadAt(p, offset); err != nil {
			return fmt.Errorf("filestore: read page %d in update: %w", pageID, err)
		}

		nSlots := p.numSlots()

		for i := uint16(0); i < nSlots; i++ {
			off, length := p.getSlot(i)
			if off == 0xFFFF || length == 0 {
				// deleted or empty
				continue
			}

			start := int(off)
			end := start + int(length)
			if end > len(p) {
				return fmt.Errorf("filestore: corrupt slot %d in update", i)
			}

			oldBytes := p[start:end]
			oldRow, err := readRowFromBytes(oldBytes, len(cols))
			if err != nil {
				return fmt.Errorf("filestore: read row in update: %w", err)
			}

			match, err := pred(oldRow)
			if err != nil {
				return err
			}
			if !match {
				continue
			}

			// Apply updater on a copy so WAL retains the original values.
			origRow := cloneRow(oldRow)
			newRow, err := updater(cloneRow(oldRow))
			if err != nil {
				return err
			}

			newBytes, err := encodeRowToBytes(newRow)
			if err != nil {
				return fmt.Errorf("filestore: encode updated row: %w", err)
			}

			if len(newBytes) <= int(length) {
				// In-place update: log UPDATE, then overwrite.
				if !tx.readOnly && tx.id != 0 {
					if err := tx.eng.wal.appendUpdate(tx.id, tableName, origRow, newRow); err != nil {
						return fmt.Errorf("filestore: WAL appendUpdate: %w", err)
					}
				}

				copy(p[start:start+len(newBytes)], newBytes)
				p.setSlot(i, off, uint16(len(newBytes)))
			} else {
				// New row is larger: log DELETE(old), delete slot, and reinsert via Insert (which logs INSERT).
				if !tx.readOnly && tx.id != 0 {
					if err := tx.eng.wal.appendDelete(tx.id, tableName, origRow); err != nil {
						return fmt.Errorf("filestore: WAL appendDelete (update-grow): %w", err)
					}
				}

				p.deleteSlot(i)
				extraRows = append(extraRows, newRow)
			}

		}

		// Write modified page back
		if _, err := f.WriteAt(p, offset); err != nil {
			return fmt.Errorf("filestore: write page %d in update: %w", pageID, err)
		}
	}

	// Reinsertion step for updated rows that did not fit in place.
	for _, r := range extraRows {
		if err := tx.Insert(tableName, r); err != nil {
			return fmt.Errorf("filestore: insert expanded updated row: %w", err)
		}
	}

	return nil
}

// Insert using a page structure
func (tx *fileTx) Insert(tableName string, row sql.Row) error {
	if tx.closed {
		return fmt.Errorf("filestore: tx is closed")
	}
	if tx.readOnly {
		return fmt.Errorf("filestore: cannot insert in read-only transaction")
	}

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

	var pageID uint32
	var slotID uint16

	writePage := func(id uint32, p pageBuf) error {
		offset := headerEnd + int64(id)*PageSize
		if _, err := f.WriteAt(p, offset); err != nil {
			return fmt.Errorf("filestore: write page %d: %w", id, err)
		}
		return nil
	}

	if numPages == 0 {
		p := newEmptyHeapPage(0)
		slotID, err = p.insertRow(rowBytes)
		if err != nil {
			return fmt.Errorf("filestore: insert into empty page: %w", err)
		}
		pageID = 0
		if err := writePage(pageID, p); err != nil {
			return err
		}
	} else {
		lastID := numPages - 1
		p := make(pageBuf, PageSize)
		offset := headerEnd + int64(lastID)*PageSize
		if _, err := f.ReadAt(p, offset); err != nil {
			return fmt.Errorf("filestore: read last page: %w", err)
		}

		slotID, err = p.insertRow(rowBytes)
		if err == nil {
			pageID = lastID
			if err := writePage(pageID, p); err != nil {
				return err
			}
		} else {
			newID := numPages
			p = newEmptyHeapPage(newID)
			slotID, err = p.insertRow(rowBytes)
			if err != nil {
				return fmt.Errorf("filestore: insert into new page: %w", err)
			}
			pageID = newID
			if err := writePage(pageID, p); err != nil {
				return err
			}
		}
	}

	// Update indexes
	tx.eng.idxMu.RLock()
	defer tx.eng.idxMu.RUnlock()

	if tableIndexes, ok := tx.eng.indexes[tableName]; ok {
		for colIdx, col := range cols {
			if idx, ok := tableIndexes[col.Name]; ok {
				val := row[colIdx]
				if val.Type != sql.TypeNull {
					rid := btree.RID{PageID: pageID, SlotID: slotID}
					if err := idx.btree.Insert(val.I64, rid); err != nil {
						return fmt.Errorf("error updating index for column %q: %w", col.Name, err)
					}
				}
			}
		}
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

	cols, err := readHeader(f)
	if err != nil {
		return fmt.Errorf("filestore: read header in replace: %w", err)
	}
	if len(cols) == 0 {
		return fmt.Errorf("filestore: replace on table %q with no columns", tableName)
	}

	for i, r := range rows {
		if len(r) != len(cols) {
			return fmt.Errorf("filestore: replace row %d length mismatch: got %d, expected %d",
				i, len(r), len(cols))
		}
	}

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

	if len(rows) > 0 {
		if err := writePage(pageID, p); err != nil {
			return err
		}
	}

	return nil
}

func cloneRow(r sql.Row) sql.Row {
	dup := make(sql.Row, len(r))
	copy(dup, r)
	return dup
}
