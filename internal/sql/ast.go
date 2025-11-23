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

// InsertStmt represents:
//
//	INSERT INTO table VALUES (...)
//	INSERT INTO table(col1, col2, ...) VALUES (...)
//
// If Columns is empty, it means "all columns in table order".
type InsertStmt struct {
	TableName string
	Columns   []string // optional; nil/empty = no column list
	Values    Row      // one row of literal values
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

// Assignment represents "column = value" in UPDATE.
type Assignment struct {
	Column string
	Value  Value
}

// UpdateStmt represents:
//
//	UPDATE tableName SET col1 = val1, col2 = val2 WHERE column = literal;
type UpdateStmt struct {
	TableName   string
	Assignments []Assignment
	Where       *WhereExpr // must not be nil for now (we require WHERE)
}

func (*UpdateStmt) stmtNode() {}

// DeleteStmt represents:
//
//	DELETE FROM tableName WHERE column = literal;
type DeleteStmt struct {
	TableName string
	Where     *WhereExpr // may be nil if you later want full-table delete; for now we require it
}

func (*DeleteStmt) stmtNode() {}

// BEGIN [TRANSACTION]
type BeginTxStmt struct{}

func (*BeginTxStmt) stmtNode() {}

// COMMIT [TRANSACTION]
type CommitTxStmt struct{}

func (*CommitTxStmt) stmtNode() {}

// ROLLBACK [TRANSACTION]
type RollbackTxStmt struct{}

func (*RollbackTxStmt) stmtNode() {}
