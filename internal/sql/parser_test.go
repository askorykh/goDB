package sql

import "testing"

func TestParseCreateTable_Basic(t *testing.T) {
	query := "CREATE TABLE users (id INT, name STRING, active BOOL);"

	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	ct, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected *CreateTableStmt, got %T", stmt)
	}

	if ct.TableName != "users" {
		t.Fatalf("expected table name %q, got %q", "users", ct.TableName)
	}

	if len(ct.Columns) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(ct.Columns))
	}

	assertCol := func(idx int, name string, dt DataType) {
		if ct.Columns[idx].Name != name {
			t.Fatalf("column %d: expected name %q, got %q", idx, name, ct.Columns[idx].Name)
		}
		if ct.Columns[idx].Type != dt {
			t.Fatalf("column %d: expected type %v, got %v", idx, dt, ct.Columns[idx].Type)
		}
	}

	assertCol(0, "id", TypeInt)
	assertCol(1, "name", TypeString)
	assertCol(2, "active", TypeBool)
}

func TestParseCreateTable_CaseAndSpaces(t *testing.T) {
	query := "  create   table   Accounts  (  balance   float ,  owner  text );  "

	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	ct, ok := stmt.(*CreateTableStmt)
	if !ok {
		t.Fatalf("expected *CreateTableStmt, got %T", stmt)
	}

	if ct.TableName != "Accounts" {
		t.Fatalf("expected table name %q, got %q", "Accounts", ct.TableName)
	}

	if len(ct.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(ct.Columns))
	}

	if ct.Columns[0].Name != "balance" || ct.Columns[0].Type != TypeFloat {
		t.Fatalf("unexpected first column: %+v", ct.Columns[0])
	}

	if ct.Columns[1].Name != "owner" || ct.Columns[1].Type != TypeString {
		t.Fatalf("unexpected second column: %+v", ct.Columns[1])
	}
}

func TestParseCreateTable_Unsupported(t *testing.T) {
	_, err := Parse("SELECT * FROM users;")
	if err == nil {
		t.Fatalf("expected error for unsupported statement, got nil")
	}
}
