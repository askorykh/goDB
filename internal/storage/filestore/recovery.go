package filestore

import (
	"encoding/binary"
	"fmt"
	"goDB/internal/sql"
	"goDB/internal/storage"
	"io"
	"os"
	"path/filepath"
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
