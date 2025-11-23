package filestore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"goDB/internal/sql"
	"goDB/internal/storage"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	fileMagic = "GODB1" // 5 bytes magic
)

type walOpType int

const (
	walOpInsert walOpType = iota
	walOpReplaceAll
)

type walOp struct {
	typ   walOpType
	table string
	rows  []sql.Row
}

type walTxState struct {
	id        uint64
	ops       []walOp
	committed bool
	rolled    bool
	order     int
}

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

func (e *FileEngine) validateTx(tx storage.Tx) (*fileTx, error) {
	if tx == nil {
		return nil, fmt.Errorf("filestore: transaction is nil")
	}

	ft, ok := tx.(*fileTx)
	if !ok {
		return nil, fmt.Errorf("filestore: invalid transaction type")
	}

	if ft.closed {
		return nil, fmt.Errorf("filestore: tx is closed")
	}

	return ft, nil
}

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

// writeHeader writes the table schema to the beginning of the file.
func writeHeader(w io.Writer, cols []sql.Column) error {
	if len(cols) > 0xFFFF {
		return fmt.Errorf("filestore: too many columns: %d", len(cols))
	}
	// magic
	if _, err := w.Write([]byte(fileMagic)); err != nil {
		return err
	}
	// numCols as uint16
	if err := binary.Write(w, binary.LittleEndian, uint16(len(cols))); err != nil {
		return err
	}

	for _, c := range cols {
		nameBytes := []byte(c.Name)
		if len(nameBytes) > 0xFFFF {
			return fmt.Errorf("column name too long: %s", c.Name)
		}
		// name length
		if err := binary.Write(w, binary.LittleEndian, uint16(len(nameBytes))); err != nil {
			return err
		}
		// name bytes
		if _, err := w.Write(nameBytes); err != nil {
			return err
		}
		// type as uint8
		if err := binary.Write(w, binary.LittleEndian, uint8(c.Type)); err != nil {
			return err
		}
	}

	return nil
}

// readHeader reads the schema from the beginning of the file and leaves
// the file position at the start of the first row.
func readHeader(r io.Reader) ([]sql.Column, error) {
	magicBuf := make([]byte, len(fileMagic))
	if _, err := io.ReadFull(r, magicBuf); err != nil {
		return nil, err
	}
	if string(magicBuf) != fileMagic {
		return nil, fmt.Errorf("filestore: invalid file magic, not a GoDB table file")
	}

	var numCols uint16
	if err := binary.Read(r, binary.LittleEndian, &numCols); err != nil {
		return nil, err
	}

	cols := make([]sql.Column, numCols)
	for i := 0; i < int(numCols); i++ {
		var nameLen uint16
		if err := binary.Read(r, binary.LittleEndian, &nameLen); err != nil {
			return nil, err
		}

		nameBytes := make([]byte, nameLen)
		if _, err := io.ReadFull(r, nameBytes); err != nil {
			return nil, err
		}

		var t uint8
		if err := binary.Read(r, binary.LittleEndian, &t); err != nil {
			return nil, err
		}

		cols[i] = sql.Column{
			Name: string(nameBytes),
			Type: sql.DataType(t),
		}
	}

	return cols, nil
}

// writeRow encodes a row as a sequence of typed values.
func writeRow(w io.Writer, row sql.Row) error {
	for _, v := range row {
		// type first
		if err := binary.Write(w, binary.LittleEndian, uint8(v.Type)); err != nil {
			return err
		}

		switch v.Type {
		case sql.TypeInt:
			if err := binary.Write(w, binary.LittleEndian, v.I64); err != nil {
				return err
			}
		case sql.TypeFloat:
			if err := binary.Write(w, binary.LittleEndian, v.F64); err != nil {
				return err
			}
		case sql.TypeString:
			b := []byte(v.S)
			if len(b) > 0xFFFFFFFF {
				return fmt.Errorf("string too long")
			}
			if err := binary.Write(w, binary.LittleEndian, uint32(len(b))); err != nil {
				return err
			}
			if _, err := w.Write(b); err != nil {
				return err
			}
		case sql.TypeBool:
			var b byte
			if v.B {
				b = 1
			}
			if err := binary.Write(w, binary.LittleEndian, b); err != nil {
				return err
			}
		case sql.TypeNull:
			// nothing else to write
		default:
			return fmt.Errorf("writeRow: unsupported value type %v", v.Type)
		}
	}

	return nil
}

// readRow decodes a row with the given number of columns.
// Returns io.EOF when there is no more data.
func readRow(r io.Reader, numCols int) (sql.Row, error) {
	row := make(sql.Row, numCols)

	for i := 0; i < numCols; i++ {
		var t uint8
		if err := binary.Read(r, binary.LittleEndian, &t); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// if we hit EOF at first column, propagate EOF;
				// if we hit mid-row, treat as error.
				if i == 0 {
					return nil, io.EOF
				}
				return nil, fmt.Errorf("readRow: truncated row")
			}
			return nil, err
		}
		vt := sql.DataType(t)

		switch vt {
		case sql.TypeInt:
			var v int64
			if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
				return nil, err
			}
			row[i] = sql.Value{Type: sql.TypeInt, I64: v}

		case sql.TypeFloat:
			var v float64
			if err := binary.Read(r, binary.LittleEndian, &v); err != nil {
				return nil, err
			}
			row[i] = sql.Value{Type: sql.TypeFloat, F64: v}

		case sql.TypeString:
			var l uint32
			if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
				return nil, err
			}
			buf := make([]byte, l)
			if _, err := io.ReadFull(r, buf); err != nil {
				return nil, err
			}
			row[i] = sql.Value{Type: sql.TypeString, S: string(buf)}

		case sql.TypeBool:
			var b byte
			if err := binary.Read(r, binary.LittleEndian, &b); err != nil {
				return nil, err
			}
			row[i] = sql.Value{Type: sql.TypeBool, B: b != 0}

		case sql.TypeNull:
			row[i] = sql.Value{Type: sql.TypeNull}

		default:
			return nil, fmt.Errorf("readRow: unsupported value type %v", vt)
		}
	}

	return row, nil
}
func (e *FileEngine) recoverFromWAL() error {
	walPath := filepath.Join(e.dir, "wal.log")

	info, err := os.Stat(walPath)
	if err != nil {
		// no WAL yet, nothing to do
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("recovery: stat WAL: %w", err)
	}

	// If WAL only has magic, nothing to replay.
	if info.Size() <= int64(len(walMagic)) {
		return nil
	}

	// 1) Load schemas for all existing tables
	tableNames, err := e.ListTables()
	if err != nil {
		return fmt.Errorf("recovery: list tables: %w", err)
	}

	schemas := make(map[string][]sql.Column)
	for _, t := range tableNames {
		cols, err := e.TableSchema(t)
		if err != nil {
			return fmt.Errorf("recovery: read schema for %q: %w", t, err)
		}
		schemas[t] = cols
	}

	// 2) Truncate data for all tables (keep header).
	for _, t := range tableNames {
		path := e.tablePath(t)
		f, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			return fmt.Errorf("recovery: open table %q: %w", t, err)
		}

		cols := schemas[t]

		if err := f.Truncate(0); err != nil {
			f.Close()
			return fmt.Errorf("recovery: truncate table %q: %w", t, err)
		}
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			f.Close()
			return fmt.Errorf("recovery: seek table %q: %w", t, err)
		}
		if err := writeHeader(f, cols); err != nil {
			f.Close()
			return fmt.Errorf("recovery: write header for %q: %w", t, err)
		}
		f.Close()
	}

	// 3) Parse WAL into tx states
	f, err := os.Open(walPath)
	if err != nil {
		return fmt.Errorf("recovery: open WAL: %w", err)
	}
	defer f.Close()

	// Skip magic
	if _, err := f.Seek(int64(len(walMagic)), io.SeekStart); err != nil {
		return fmt.Errorf("recovery: seek WAL: %w", err)
	}

	txStates := make(map[uint64]*walTxState)
	var txOrder []uint64
	getTx := func(id uint64) *walTxState {
		if s, ok := txStates[id]; ok {
			return s
		}
		s := &walTxState{id: id, order: len(txOrder)}
		txStates[id] = s
		txOrder = append(txOrder, id)
		return s
	}

	for {
		var recType uint8
		if err := binary.Read(f, binary.LittleEndian, &recType); err != nil {
			if err == io.EOF {
				break
			}
			return fmt.Errorf("recovery: read recType: %w", err)
		}

		var txID uint64
		if err := binary.Read(f, binary.LittleEndian, &txID); err != nil {
			return fmt.Errorf("recovery: read txID: %w", err)
		}
		txState := getTx(txID)

		switch recType {
		case walRecBegin:
			// nothing more in payload
		case walRecCommit:
			txState.committed = true
		case walRecRollback:
			txState.rolled = true
		case walRecInsert, walRecReplaceAll:
			var nameLen uint16
			if err := binary.Read(f, binary.LittleEndian, &nameLen); err != nil {
				return fmt.Errorf("recovery: read table name len: %w", err)
			}
			nameBytes := make([]byte, nameLen)
			if _, err := io.ReadFull(f, nameBytes); err != nil {
				return fmt.Errorf("recovery: read table name: %w", err)
			}
			table := string(nameBytes)

			var rowCount uint32
			if err := binary.Read(f, binary.LittleEndian, &rowCount); err != nil {
				return fmt.Errorf("recovery: read rowCount: %w", err)
			}

			cols, ok := schemas[table]
			if !ok {
				// table doesn't exist anymore; skip rows
				for i := uint32(0); i < rowCount; i++ {
					// read and discard
					_, _ = readRow(f, 0) // but readRow needs numCols; so instead:
					return fmt.Errorf("recovery: table %q in WAL but not in schema", table)
				}
				continue
			}

			rows := make([]sql.Row, 0, rowCount)
			for i := uint32(0); i < rowCount; i++ {
				r, err := readRow(f, len(cols))
				if err != nil {
					return fmt.Errorf("recovery: read row: %w", err)
				}
				rows = append(rows, r)
			}

			opType := walOpInsert
			if recType == walRecReplaceAll {
				opType = walOpReplaceAll
			}
			txState.ops = append(txState.ops, walOp{
				typ:   opType,
				table: table,
				rows:  rows,
			})

		default:
			return fmt.Errorf("recovery: unknown WAL record type %d", recType)
		}
	}

	// 4) Apply committed txs in log order
	// (ignore rolled back or incomplete txs)
	for _, txID := range txOrder {
		s := txStates[txID]
		if !s.committed || s.rolled {
			continue
		}
		if err := e.applyTxOps(s, schemas); err != nil {
			return fmt.Errorf("recovery: apply tx %d: %w", txID, err)
		}
	}

	return nil
}
func (e *FileEngine) applyTxOps(s *walTxState, schemas map[string][]sql.Column) error {
	for _, op := range s.ops {
		switch op.typ {
		case walOpInsert:
			path := e.tablePath(op.table)
			f, err := os.OpenFile(path, os.O_RDWR, 0o644)
			if err != nil {
				return fmt.Errorf("recovery: open table %q for insert: %w", op.table, err)
			}

			if _, err := f.Seek(0, io.SeekEnd); err != nil {
				f.Close()
				return fmt.Errorf("recovery: seek end for %q: %w", op.table, err)
			}
			for _, r := range op.rows {
				if err := writeRow(f, r); err != nil {
					f.Close()
					return fmt.Errorf("recovery: write row for %q: %w", op.table, err)
				}
			}
			f.Close()

		case walOpReplaceAll:
			path := e.tablePath(op.table)
			f, err := os.OpenFile(path, os.O_RDWR, 0o644)
			if err != nil {
				return fmt.Errorf("recovery: open table %q for replace: %w", op.table, err)
			}
			cols := schemas[op.table]

			if err := f.Truncate(0); err != nil {
				f.Close()
				return fmt.Errorf("recovery: truncate table %q: %w", op.table, err)
			}
			if _, err := f.Seek(0, io.SeekStart); err != nil {
				f.Close()
				return fmt.Errorf("recovery: seek table %q: %w", op.table, err)
			}
			if err := writeHeader(f, cols); err != nil {
				f.Close()
				return fmt.Errorf("recovery: write header for %q: %w", op.table, err)
			}
			for _, r := range op.rows {
				if err := writeRow(f, r); err != nil {
					f.Close()
					return fmt.Errorf("recovery: write row for %q: %w", op.table, err)
				}
			}
			f.Close()
		}
	}
	return nil
}
