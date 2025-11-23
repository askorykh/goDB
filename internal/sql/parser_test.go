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
func TestParseSelect_ColumnList(t *testing.T) {
	query := "SELECT id, name FROM users;"

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
	if sel.Where != nil {
		t.Fatalf("expected no WHERE, got %+v", sel.Where)
	}
	if len(sel.Columns) != 2 || sel.Columns[0] != "id" || sel.Columns[1] != "name" {
		t.Fatalf("unexpected Columns: %#v", sel.Columns)
	}
}

func TestParseSelect_ColumnListWithWhere(t *testing.T) {
	query := "SELECT id, name FROM users WHERE active = true;"

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
	if len(sel.Columns) != 2 || sel.Columns[0] != "id" || sel.Columns[1] != "name" {
		t.Fatalf("unexpected Columns: %#v", sel.Columns)
	}
}
func TestParseUpdate_Basic(t *testing.T) {
	query := "UPDATE users SET active = false WHERE id = 1;"

	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	upd, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("expected *UpdateStmt, got %T", stmt)
	}

	if upd.TableName != "users" {
		t.Fatalf("expected table name %q, got %q", "users", upd.TableName)
	}

	if upd.Where == nil {
		t.Fatalf("expected WHERE clause, got nil")
	}
	if upd.Where.Column != "id" || upd.Where.Op != "=" {
		t.Fatalf("unexpected WHERE expr: %+v", upd.Where)
	}
	if upd.Where.Value.Type != TypeInt || upd.Where.Value.I64 != 1 {
		t.Fatalf("unexpected WHERE value: %+v", upd.Where.Value)
	}

	if len(upd.Assignments) != 1 {
		t.Fatalf("expected 1 assignment, got %d", len(upd.Assignments))
	}
	assign := upd.Assignments[0]
	if assign.Column != "active" {
		t.Fatalf("expected assignment column %q, got %q", "active", assign.Column)
	}
	if assign.Value.Type != TypeBool || assign.Value.B != false {
		t.Fatalf("unexpected assignment value: %+v", assign.Value)
	}
}

func TestParseUpdate_MultiAssignmentWithSpaces(t *testing.T) {
	query := "  update   users   set   name = 'Alice',  active = true   where   id = 42 ;"

	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	upd, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("expected *UpdateStmt, got %T", stmt)
	}

	if upd.TableName != "users" {
		t.Fatalf("expected table name %q, got %q", "users", upd.TableName)
	}

	if upd.Where == nil {
		t.Fatalf("expected WHERE clause, got nil")
	}
	if upd.Where.Column != "id" || upd.Where.Value.Type != TypeInt || upd.Where.Value.I64 != 42 {
		t.Fatalf("unexpected WHERE: %+v", upd.Where)
	}

	if len(upd.Assignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(upd.Assignments))
	}

	// we don't enforce order strongly, but likely: name, active
	a0, a1 := upd.Assignments[0], upd.Assignments[1]

	if a0.Column != "name" || a0.Value.Type != TypeString || a0.Value.S != "Alice" {
		t.Fatalf("unexpected first assignment: %+v", a0)
	}
	if a1.Column != "active" || a1.Value.Type != TypeBool || a1.Value.B != true {
		t.Fatalf("unexpected second assignment: %+v", a1)
	}
}
func TestParseDelete_Basic(t *testing.T) {
	query := "DELETE FROM users WHERE id = 1;"

	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	del, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("expected *DeleteStmt, got %T", stmt)
	}

	if del.TableName != "users" {
		t.Fatalf("expected table name %q, got %q", "users", del.TableName)
	}

	if del.Where == nil {
		t.Fatalf("expected WHERE clause, got nil")
	}
	if del.Where.Column != "id" || del.Where.Value.Type != TypeInt || del.Where.Value.I64 != 1 {
		t.Fatalf("unexpected WHERE: %+v", del.Where)
	}
}

func TestParseDelete_WithSpaces(t *testing.T) {
	query := "  delete   from   Accounts   where   active = false ; "

	stmt, err := Parse(query)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	del, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("expected *DeleteStmt, got %T", stmt)
	}

	if del.TableName != "Accounts" {
		t.Fatalf("expected table name %q, got %q", "Accounts", del.TableName)
	}

	if del.Where == nil {
		t.Fatalf("expected WHERE clause, got nil")
	}
	if del.Where.Column != "active" || del.Where.Value.Type != TypeBool || del.Where.Value.B != false {
		t.Fatalf("unexpected WHERE: %+v", del.Where)
	}
}
