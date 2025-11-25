package sql

// DataType represents the logical type of a value in a column.
type DataType int

const (
	TypeInt DataType = iota
	TypeFloat
	TypeString
	TypeBool
)

// Value represents a single cell in a table (one column in one row).
// Only the field matching Type should be read; other fields remain at their
// zero values to keep the struct compact and easy to inspect while debugging.
type Value struct {
	Type DataType

	I64 int64   // for TypeInt
	F64 float64 // for TypeFloat
	S   string  // for TypeString
	B   bool    // for TypeBool
}

// Row represents one record in a table: a slice of Values, one per column.
type Row []Value

// Column describes metadata for a single column in a table.
type Column struct {
	Name string
	Type DataType
}
