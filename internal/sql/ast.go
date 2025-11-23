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
