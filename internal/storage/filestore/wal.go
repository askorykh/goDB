package filestore

import (
	"encoding/binary"
	"fmt"
	"goDB/internal/sql"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	walMagic = "GODBWAL1" // 8 bytes

	walRecInsert     = 1
	walRecReplaceAll = 2
)

// walLogger is a very simple write-ahead log:
//
// File layout:
//
//	[magic "GODBWAL1"]
//	[records...]
//
// Each record:
//
//	recType:      uint8
//	tableNameLen: uint16
//	tableName:    tableNameLen bytes
//	rowCount:     uint32
//	row data:     repeated rowCount times (same encoding as table rows).
type walLogger struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// newWAL opens or creates WAL file in append mode and ensures magic header.
func newWAL(dir string) (*walLogger, error) {
	path := filepath.Join(dir, "wal.log")

	// Open or create
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}

	// Check if file is new/empty; if so, write magic.
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: stat: %w", err)
	}

	if info.Size() == 0 {
		if _, err := f.Write([]byte(walMagic)); err != nil {
			f.Close()
			return nil, fmt.Errorf("wal: write magic: %w", err)
		}
	} else {
		// verify magic
		magicBuf := make([]byte, len(walMagic))
		if _, err := f.ReadAt(magicBuf, 0); err != nil {
			f.Close()
			return nil, fmt.Errorf("wal: read magic: %w", err)
		}
		if string(magicBuf) != walMagic {
			f.Close()
			return nil, fmt.Errorf("wal: invalid magic, not a GoDB WAL file")
		}
	}

	// Seek to end for append
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: seek end: %w", err)
	}

	return &walLogger{
		f:    f,
		path: path,
	}, nil
}

// Close closes the WAL file.
func (w *walLogger) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

// Sync flushes WAL to disk.
func (w *walLogger) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return fmt.Errorf("wal: closed")
	}
	return w.f.Sync()
}

func (w *walLogger) appendInsert(table string, row sql.Row) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return fmt.Errorf("wal: closed")
	}

	if err := w.writeRecordHeader(walRecInsert, table, 1); err != nil {
		return err
	}
	if err := writeRow(w.f, row); err != nil {
		return fmt.Errorf("wal: write row: %w", err)
	}
	return nil
}

func (w *walLogger) appendReplaceAll(table string, rows []sql.Row) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return fmt.Errorf("wal: closed")
	}

	if err := w.writeRecordHeader(walRecReplaceAll, table, len(rows)); err != nil {
		return err
	}
	for _, r := range rows {
		if err := writeRow(w.f, r); err != nil {
			return fmt.Errorf("wal: write row: %w", err)
		}
	}
	return nil
}

func (w *walLogger) writeRecordHeader(recType uint8, table string, rowCount int) error {
	if w.f == nil {
		return fmt.Errorf("wal: closed")
	}

	if err := binary.Write(w.f, binary.LittleEndian, recType); err != nil {
		return err
	}

	nameBytes := []byte(table)
	if len(nameBytes) > 0xFFFF {
		return fmt.Errorf("wal: table name too long")
	}
	if err := binary.Write(w.f, binary.LittleEndian, uint16(len(nameBytes))); err != nil {
		return err
	}
	if _, err := w.f.Write(nameBytes); err != nil {
		return err
	}

	if err := binary.Write(w.f, binary.LittleEndian, uint32(rowCount)); err != nil {
		return err
	}

	return nil
}
