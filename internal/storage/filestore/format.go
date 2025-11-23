package filestore

import (
	"encoding/binary"
	"fmt"
	"goDB/internal/sql"
	"io"
)

const (
	fileMagic = "GODB1" // 5 bytes magic
)

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
