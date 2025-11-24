package filestore

import (
	"errors"
	"fmt"
	"goDB/internal/index/btree"
	"goDB/internal/sql"
	"goDB/internal/storage"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type indexInfo struct {
	name       string
	tableName  string
	columnName string
	btree      btree.Index
}

// FileEngine is a simple on-disk storage engine.
type FileEngine struct {
	dir string
	wal *walLogger

	mu       sync.Mutex
	nextTxID uint64
	indexMgr *btree.Manager

	idxMu   sync.RWMutex
	indexes map[string]map[string]*indexInfo // tableName -> columnName -> info
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
		indexes:  make(map[string]map[string]*indexInfo),
	}

	e.indexMgr = btree.NewManager(dir)

	// Load existing indexes from disk.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("filestore: read dir to load indexes: %w", err)
	}
	for _, ent := range entries {
		name := ent.Name()
		if strings.HasSuffix(name, ".idx") {
			parts := strings.Split(strings.TrimSuffix(name, ".idx"), "_")
			if len(parts) == 2 {
				tableName := parts[0]
				columnName := parts[1]

				bt, err := e.indexMgr.OpenOrCreateIndex(tableName, columnName)
				if err != nil {
					return nil, fmt.Errorf("filestore: could not open existing index %s: %w", name, err)
				}
				if e.indexes[tableName] == nil {
					e.indexes[tableName] = make(map[string]*indexInfo)
				}
				e.indexes[tableName][columnName] = &indexInfo{
					name:       name, // Use filename as internal name
					tableName:  tableName,
					columnName: columnName,
					btree:      bt,
				}
			}
		}
	}

	// Recover database state from WAL on startup.
	if err := e.recoverFromWAL(); err != nil {
		return nil, fmt.Errorf("filestore: recovery failed: %w", err)
	}

	return e, nil
}

func (e *FileEngine) CreateIndex(indexName, tableName, columnName string) error {
	e.idxMu.RLock()
	if columns, ok := e.indexes[tableName]; ok {
		if _, exists := columns[columnName]; exists {
			e.idxMu.RUnlock()
			return fmt.Errorf("filestore: index on %s.%s already exists", tableName, columnName)
		}
	}
	e.idxMu.RUnlock()

	path := e.tablePath(tableName)
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("filestore: open table for index creation: %w", err)
	}
	defer f.Close()

	cols, err := readHeader(f)
	if err != nil {
		return fmt.Errorf("filestore: read header for index creation: %w", err)
	}
	headerEnd, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return fmt.Errorf("filestore: seek after header for index creation: %w", err)
	}

	colIdx := -1
	for i, c := range cols {
		if strings.EqualFold(c.Name, columnName) {
			colIdx = i
			break
		}
	}
	if colIdx == -1 {
		return fmt.Errorf("filestore: column %q not found in table %q", columnName, tableName)
	}
	if cols[colIdx].Type != sql.TypeInt {
		return fmt.Errorf("filestore: cannot create index on non-integer column %q", columnName)
	}

	bt, err := e.indexMgr.OpenOrCreateIndex(tableName, columnName)
	if err != nil {
		return fmt.Errorf("filestore: could not create index: %w", err)
	}

	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("filestore: stat table for index creation: %w", err)
	}
	fileSize := fi.Size()
	if fileSize < headerEnd {
		return fmt.Errorf("filestore: corrupt file, size < header")
	}
	dataBytes := fileSize - headerEnd
	if dataBytes > 0 {
		if dataBytes%PageSize != 0 {
			return fmt.Errorf("filestore: corrupt data (not multiple of page size)")
		}
		numPages := uint32(dataBytes / PageSize)

		for pageID := uint32(0); pageID < numPages; pageID++ {
			p := make(pageBuf, PageSize)
			offset := headerEnd + int64(pageID)*PageSize
			if _, err := f.ReadAt(p, offset); err != nil {
				return fmt.Errorf("filestore: read page %d for index creation: %w", pageID, err)
			}

			err := p.iterateRows(len(cols), func(slotID uint16, r sql.Row) error {
				val := r[colIdx]
				if val.Type == sql.TypeNull {
					return nil
				}
				rid := btree.RID{PageID: pageID, SlotID: slotID}
				if err := bt.Insert(val.I64, rid); err != nil {
					return fmt.Errorf("error building index: %w", err)
				}
				return nil
			})
			if err != nil {
				return fmt.Errorf("filestore: iterate rows in page %d for index creation: %w", pageID, err)
			}
		}
	}

	// Register the index in the engine's in-memory map.
	e.idxMu.Lock()
	defer e.idxMu.Unlock()

	if e.indexes[tableName] == nil {
		e.indexes[tableName] = make(map[string]*indexInfo)
	}
	e.indexes[tableName][columnName] = &indexInfo{
		name:       indexName,
		tableName:  tableName,
		columnName: columnName,
		btree:      bt,
	}

	return nil
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

func (e *FileEngine) tablePath(name string) string {
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
func (e *FileEngine) Begin(readOnly bool) (storage.Tx, error) {
	tx := &fileTx{
		eng:      e,
		readOnly: readOnly,
		closed:   false,
		id:       0,
	}

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
