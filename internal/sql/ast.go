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
// For now we only support:
//
//	SELECT * FROM tableName;
type SelectStmt struct {
	TableName string
}

func (*SelectStmt) stmtNode() {}
