package sql

// Statement is the common interface for all SQL statements.
type Statement interface {
	stmtNode()
}

// CreateTableStmt represents a parsed CREATE TABLE statement.
type CreateTableStmt struct {
	TableName string
	Columns   []Column
}

func (*CreateTableStmt) stmtNode() {}

// InsertStmt represents a parsed INSERT INTO ... VALUES (...) statement.
type InsertStmt struct {
	TableName string
	Values    Row // one row of literal values
}

func (*InsertStmt) stmtNode() {}

// SelectStmt represents a parsed SELECT statement.
// Supported forms (for now):
//
//	SELECT * FROM table;
//	SELECT col1, col2 FROM table;
//	... optionally with WHERE column = literal
type SelectStmt struct {
	TableName string
	Columns   []string   // nil or empty => SELECT *
	Where     *WhereExpr // nil if no WHERE clause
}

func (*SelectStmt) stmtNode() {}

// WhereExpr represents a simple WHERE condition: column = literal.
type WhereExpr struct {
	Column string
	Op     string // currently only "=" is supported
	Value  Value
}
