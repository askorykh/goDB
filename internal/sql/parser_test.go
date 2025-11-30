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

func TestParseInsert_Basic(t *testing.T) {
	query := "INSERT INTO users VALUES (1, 'Alice', true);"

	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	ins, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("expected *InsertStmt, got %T", stmt)
	}

	if ins.TableName != "users" {
		t.Fatalf("expected table name %q, got %q", "users", ins.TableName)
	}

	if len(ins.Values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(ins.Values))
	}

	// id
	if ins.Values[0].Type != TypeInt || ins.Values[0].I64 != 1 {
		t.Fatalf("unexpected first value: %+v", ins.Values[0])
	}
	// name
	if ins.Values[1].Type != TypeString || ins.Values[1].S != "Alice" {
		t.Fatalf("unexpected second value: %+v", ins.Values[1])
	}
	// active
	if ins.Values[2].Type != TypeBool || ins.Values[2].B != true {
		t.Fatalf("unexpected third value: %+v", ins.Values[2])
	}
}

func TestParseInsert_CaseAndSpaces(t *testing.T) {
	query := "  insert  into   Accounts   values  (  100.5 ,  'John Doe' , FALSE ); "

	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	ins, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("expected *InsertStmt, got %T", stmt)
	}

	if ins.TableName != "Accounts" {
		t.Fatalf("expected table name %q, got %q", "Accounts", ins.TableName)
	}

	if len(ins.Values) != 3 {
		t.Fatalf("expected 3 values, got %d", len(ins.Values))
	}

	if ins.Values[0].Type != TypeFloat {
		t.Fatalf("expected first value to be FLOAT, got %v", ins.Values[0].Type)
	}
	if ins.Values[1].Type != TypeString || ins.Values[1].S != "John Doe" {
		t.Fatalf("unexpected second value: %+v", ins.Values[1])
	}
	if ins.Values[2].Type != TypeBool || ins.Values[2].B != false {
		t.Fatalf("unexpected third value: %+v", ins.Values[2])
	}
}
func TestParseSelect_Basic(t *testing.T) {
	query := "SELECT * FROM users;"

	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if sel.TableName != "users" {
		t.Fatalf("expected table name %q, got %q", "users", sel.TableName)
	}
}

func TestParseSelect_CaseAndSpaces(t *testing.T) {
	query := "   select   *   from   Accounts   ; "

	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if sel.TableName != "Accounts" {
		t.Fatalf("expected table name %q, got %q", "Accounts", sel.TableName)
	}
}

func TestParseSelect_OnlyStarSupported(t *testing.T) {
	_, err := Parse("SELECT id, name FROM users;")
	if err == nil {
		t.Fatalf("expected error for non-* SELECT, got nil")
	}
}

func TestParseSelect_WithWhereInt(t *testing.T) {
	query := "SELECT * FROM users WHERE id = 1;"

	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if sel.TableName != "users" {
		t.Fatalf("expected table name %q, got %q", "users", sel.TableName)
	}
	if sel.Where == nil {
		t.Fatalf("expected WHERE clause, got nil")
	}
	if sel.Where.Column != "id" || sel.Where.Op != "=" {
		t.Fatalf("unexpected WHERE expr: %+v", sel.Where)
	}
	if sel.Where.Value.Type != TypeInt || sel.Where.Value.I64 != 1 {
		t.Fatalf("unexpected WHERE value: %+v", sel.Where.Value)
	}
}

func TestParseSelect_WithWhereString(t *testing.T) {
	query := "  select * from  users  where  name = 'Alice Smith' ; "

	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	sel, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if sel.TableName != "users" {
		t.Fatalf("expected table name %q, got %q", "users", sel.TableName)
	}
	if sel.Where == nil {
		t.Fatalf("expected WHERE clause, got nil")
	}
	if sel.Where.Column != "name" || sel.Where.Op != "=" {
		t.Fatalf("unexpected WHERE expr: %+v", sel.Where)
	}
	if sel.Where.Value.Type != TypeString || sel.Where.Value.S != "Alice Smith" {
		t.Fatalf("unexpected WHERE value: %+v", sel.Where.Value)
	}
}
