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

// WAL file format (version 2):
//
//   magic: "GODBWAL2" (8 bytes)
//
//   then a sequence of records:
//     recType: uint8
//     txID:    uint64
//     ... type-specific payload ...
//
//   Types:
//     BEGIN:      recType = 1, payload: none
//     COMMIT:     recType = 2, payload: none
//     ROLLBACK:   recType = 3, payload: none
//     INSERT:     recType = 4, payload:
//                  tableNameLen: uint16
//                  tableName:    bytes
//                  rowCount:     uint32 (must be 1 for INSERT)
//                  row data:     encoded row (see writeRow)
//     REPLACEALL: recType = 5, payload:
//                  tableNameLen: uint16
//                  tableName:    bytes
//                  rowCount:     uint32
//                  row data:     repeated rowCount times

const (
	walMagic = "GODBWAL2"

	walRecBegin      uint8 = 1
	walRecCommit     uint8 = 2
	walRecRollback   uint8 = 3
	walRecInsert     uint8 = 4
	walRecReplaceAll uint8 = 5
	walRecDelete     uint8 = 6
	walRecUpdate     uint8 = 7
)

// walLogger is a simple append-only WAL writer.
type walLogger struct {
	mu   sync.Mutex
	f    *os.File
	path string
}

// newWAL opens or creates WAL file and ensures correct magic header.
func newWAL(dir string) (*walLogger, error) {
	path := filepath.Join(dir, "wal.log")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: stat: %w", err)
	}

	if info.Size() == 0 {
		// new file -> write magic
		if _, err := f.Write([]byte(walMagic)); err != nil {
			f.Close()
			return nil, fmt.Errorf("wal: write magic: %w", err)
		}
	} else {
		// existing file -> verify magic
		magicBuf := make([]byte, len(walMagic))
		if _, err := f.ReadAt(magicBuf, 0); err != nil {
			f.Close()
			return nil, fmt.Errorf("wal: read magic: %w", err)
		}
		if string(magicBuf) != walMagic {
			f.Close()
			return nil, fmt.Errorf("wal: invalid magic, not a GoDB WAL v2 file")
		}
	}

	// Seek to end for appends
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

// appendBegin writes a BEGIN record for txID.
func (w *walLogger) appendBegin(txID uint64) error {
	return w.appendNoPayload(walRecBegin, txID)
}

// appendCommit writes a COMMIT record for txID.
func (w *walLogger) appendCommit(txID uint64) error {
	return w.appendNoPayload(walRecCommit, txID)
}

// appendRollback writes a ROLLBACK record for txID.
func (w *walLogger) appendRollback(txID uint64) error {
	return w.appendNoPayload(walRecRollback, txID)
}

func (w *walLogger) appendNoPayload(recType uint8, txID uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return fmt.Errorf("wal: closed")
	}

	// recType
	if err := binary.Write(w.f, binary.LittleEndian, recType); err != nil {
		return err
	}
	// txID
	if err := binary.Write(w.f, binary.LittleEndian, txID); err != nil {
		return err
	}
	return nil
}

// appendInsert logs an INSERT record for txID.
func (w *walLogger) appendInsert(txID uint64, table string, row sql.Row) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return fmt.Errorf("wal: closed")
	}

	if err := w.writeRecordHeader(txID, walRecInsert, table, 1); err != nil {
		return err
	}
	if err := writeRow(w.f, row); err != nil {
		return fmt.Errorf("wal: write row: %w", err)
	}
	return nil
}

// appendReplaceAll logs a REPLACEALL record for txID.
func (w *walLogger) appendReplaceAll(txID uint64, table string, rows []sql.Row) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return fmt.Errorf("wal: closed")
	}

	if err := w.writeRecordHeader(txID, walRecReplaceAll, table, len(rows)); err != nil {
		return err
	}
	for _, r := range rows {
		if err := writeRow(w.f, r); err != nil {
			return fmt.Errorf("wal: write row: %w", err)
		}
	}
	return nil
}

func (w *walLogger) writeRecordHeader(txID uint64, recType uint8, table string, rowCount int) error {
	if w.f == nil {
		return fmt.Errorf("wal: closed")
	}

	// recType
	if err := binary.Write(w.f, binary.LittleEndian, recType); err != nil {
		return err
	}
	// txID
	if err := binary.Write(w.f, binary.LittleEndian, txID); err != nil {
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
func (w *walLogger) appendDelete(txID uint64, table string, row sql.Row) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return fmt.Errorf("wal: closed")
	}

	if err := w.writeRecordHeader(txID, walRecDelete, table, 1); err != nil {
		return err
	}
	if err := writeRow(w.f, row); err != nil {
		return fmt.Errorf("wal: write delete row: %w", err)
	}
	return nil
}

func (w *walLogger) appendUpdate(txID uint64, table string, oldRow, newRow sql.Row) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return fmt.Errorf("wal: closed")
	}

	// rowCount = 2: [oldRow, newRow]
	if err := w.writeRecordHeader(txID, walRecUpdate, table, 2); err != nil {
		return err
	}
	if err := writeRow(w.f, oldRow); err != nil {
		return fmt.Errorf("wal: write old row in update: %w", err)
	}
	if err := writeRow(w.f, newRow); err != nil {
		return fmt.Errorf("wal: write new row in update: %w", err)
	}
	return nil
}
