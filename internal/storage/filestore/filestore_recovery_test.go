package filestore

import (
	"goDB/internal/sql"
	"goDB/internal/storage"
	"os"
	"path/filepath"
	"testing"
)

// Helper: read all rows from a table using a read-only tx.
func scanAll(t *testing.T, fs *FileEngine, table string) ([]string, []sql.Row) {
	t.Helper()
	tx, err := fs.Begin(true)
	if err != nil {
		t.Fatalf("Begin(readOnly) failed: %v", err)
	}
	defer fs.Commit(tx) // no-op for readOnly, but keeps API consistent

	cols, rows, err := tx.Scan(table)
	if err != nil {
		t.Fatalf("Scan(%q) failed: %v", table, err)
	}
	return cols, rows
}

// Recovery should replay committed INSERTs from WAL on startup.
func TestFilestore_Recovery_ReplaysCommittedInserts(t *testing.T) {
	dir := t.TempDir()

	// First "process": create engine, table, insert data, commit.
	fs1, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs1) failed: %v", err)
	}

	cols := []sql.Column{
		{Name: "id", Type: sql.TypeInt},
		{Name: "name", Type: sql.TypeString},
	}

	if err := fs1.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable(users) failed: %v", err)
	}

	// tx1: insert two rows and commit
	tx1, err := fs1.Begin(false)
	if err != nil {
		t.Fatalf("Begin(tx1) failed: %v", err)
	}
	_ = tx1.Insert("users", sql.Row{
		{Type: sql.TypeInt, I64: 1},
		{Type: sql.TypeString, S: "Alice"},
	})
	_ = tx1.Insert("users", sql.Row{
		{Type: sql.TypeInt, I64: 2},
		{Type: sql.TypeString, S: "Bob"},
	})
	if err := fs1.Commit(tx1); err != nil {
		t.Fatalf("Commit(tx1) failed: %v", err)
	}

	// Optional: verify pre-restart state
	_, rowsBefore := scanAll(t, fs1, "users")
	if len(rowsBefore) != 2 {
		t.Fatalf("before restart: expected 2 rows, got %d", len(rowsBefore))
	}

	// "Restart": create a new engine instance pointing to the same dir.
	fs2, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs2) failed: %v", err)
	}

	colsAfter, rowsAfter := scanAll(t, fs2, "users")
	if len(colsAfter) != 2 || colsAfter[0] != "id" || colsAfter[1] != "name" {
		t.Fatalf("after restart: unexpected cols: %v", colsAfter)
	}

	if len(rowsAfter) != 2 {
		t.Fatalf("after restart: expected 2 rows, got %d", len(rowsAfter))
	}

	ids := []int64{rowsAfter[0][0].I64, rowsAfter[1][0].I64}
	names := []string{rowsAfter[0][1].S, rowsAfter[1][1].S}

	// Order should be preserved by simple replay (tx only used INSERTs).
	if ids[0] != 1 || ids[1] != 2 || names[0] != "Alice" || names[1] != "Bob" {
		t.Fatalf("after restart: unexpected data: ids=%v, names=%v", ids, names)
	}
}

// Recovery should ignore rolled-back transactions: data they wrote should disappear after restart.
func TestFilestore_Recovery_IgnoresRolledBackTx(t *testing.T) {
	dir := t.TempDir()

	fs1, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs1) failed: %v", err)
	}

	cols := []sql.Column{
		{Name: "id", Type: sql.TypeInt},
	}

	if err := fs1.CreateTable("t", cols); err != nil {
		t.Fatalf("CreateTable(t) failed: %v", err)
	}

	// tx1: committed insert of id=1
	tx1, err := fs1.Begin(false)
	if err != nil {
		t.Fatalf("Begin(tx1) failed: %v", err)
	}
	_ = tx1.Insert("t", sql.Row{{Type: sql.TypeInt, I64: 1}})
	if err := fs1.Commit(tx1); err != nil {
		t.Fatalf("Commit(tx1) failed: %v", err)
	}

	// tx2: insert id=2 but rollback
	tx2, err := fs1.Begin(false)
	if err != nil {
		t.Fatalf("Begin(tx2) failed: %v", err)
	}
	_ = tx2.Insert("t", sql.Row{{Type: sql.TypeInt, I64: 2}})
	if err := fs1.Rollback(tx2); err != nil {
		t.Fatalf("Rollback(tx2) failed: %v", err)
	}

	// Before restart, because our current filestore writes directly to the table
	// even for tx2, we may see both rows:
	_, rowsBefore := scanAll(t, fs1, "t")
	if len(rowsBefore) != 2 {
		t.Fatalf("before restart: expected 2 rows (no undo), got %d", len(rowsBefore))
	}

	// Restart: recovery should rebuild table only from committed txs.
	fs2, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs2) failed: %v", err)
	}

	_, rowsAfter := scanAll(t, fs2, "t")
	if len(rowsAfter) != 1 {
		t.Fatalf("after restart: expected 1 row (rolled-back tx ignored), got %d", len(rowsAfter))
	}
	if rowsAfter[0][0].I64 != 1 {
		t.Fatalf("after restart: expected id=1, got %d", rowsAfter[0][0].I64)
	}
}

// Recovery should use WAL only if present and non-empty.
func TestFilestore_Recovery_NoWalFileIsNoop(t *testing.T) {
	dir := t.TempDir()

	// First start: no WAL yet, New should succeed and recovery do nothing.
	fs1, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs1) failed: %v", err)
	}

	cols := []sql.Column{
		{Name: "id", Type: sql.TypeInt},
	}
	if err := fs1.CreateTable("t", cols); err != nil {
		t.Fatalf("CreateTable(t) failed: %v", err)
	}

	// No writes, no WAL records.
	// Restart: must not error, and table should still exist with empty rows.
	fs2, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs2) failed: %v", err)
	}

	tables, err := fs2.ListTables()
	if err != nil {
		t.Fatalf("ListTables failed: %v", err)
	}
	if len(tables) != 1 || tables[0] != "t" {
		t.Fatalf("unexpected tables after restart: %v", tables)
	}

	_, rows := scanAll(t, fs2, "t")
	if len(rows) != 0 {
		t.Fatalf("expected no rows in t after restart, got %d", len(rows))
	}
}

// Optional sanity check: WAL file exists and is non-empty after some writes.
func TestFilestore_Recovery_WalExistsAndGrows(t *testing.T) {
	dir := t.TempDir()

	fs, err := New(dir)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	cols := []sql.Column{
		{Name: "id", Type: sql.TypeInt},
	}
	if err := fs.CreateTable("t", cols); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	tx, _ := fs.Begin(false)
	_ = tx.Insert("t", sql.Row{{Type: sql.TypeInt, I64: 123}})
	if err := fs.Commit(tx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	walPath := filepath.Join(dir, "wal.log")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("wal.log not found: %v", err)
	}
	if info.Size() <= int64(len("GODBWAL2")) {
		t.Fatalf("wal.log too small, no records? size=%d", info.Size())
	}
}
func TestFilestore_Recovery_Delete_Replayed(t *testing.T) {
	dir := t.TempDir()

	fs1, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs1) failed: %v", err)
	}

	cols := []sql.Column{
		{Name: "id", Type: sql.TypeInt},
	}
	if err := fs1.CreateTable("t", cols); err != nil {
		t.Fatalf("CreateTable(t) failed: %v", err)
	}

	// Insert id=1 and id=2 in committed tx
	tx1, _ := fs1.Begin(false)
	_ = tx1.Insert("t", sql.Row{{Type: sql.TypeInt, I64: 1}})
	_ = tx1.Insert("t", sql.Row{{Type: sql.TypeInt, I64: 2}})
	if err := fs1.Commit(tx1); err != nil {
		t.Fatalf("Commit(tx1) failed: %v", err)
	}

	// Delete id=2 and commit
	tx2, _ := fs1.Begin(false)
	pred := func(row sql.Row) (bool, error) {
		return row[0].I64 == 2, nil
	}
	if err := tx2.DeleteWhere("t", storage.RowPredicate(pred)); err != nil {
		t.Fatalf("DeleteWhere failed: %v", err)
	}
	if err := fs1.Commit(tx2); err != nil {
		t.Fatalf("Commit(tx2) failed: %v", err)
	}

	// Restart
	fs2, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs2) failed: %v", err)
	}

	_, rows := scanAll(t, fs2, "t")
	if len(rows) != 1 || rows[0][0].I64 != 1 {
		t.Fatalf("after restart: expected only id=1, got rows=%v", rows)
	}
}
func TestFilestore_Recovery_Delete_RollbackIgnored(t *testing.T) {
	dir := t.TempDir()

	fs1, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs1) failed: %v", err)
	}

	cols := []sql.Column{
		{Name: "id", Type: sql.TypeInt},
	}
	if err := fs1.CreateTable("t", cols); err != nil {
		t.Fatalf("CreateTable(t) failed: %v", err)
	}

	// Insert committed rows: 1,2
	tx1, _ := fs1.Begin(false)
	_ = tx1.Insert("t", sql.Row{{Type: sql.TypeInt, I64: 1}})
	_ = tx1.Insert("t", sql.Row{{Type: sql.TypeInt, I64: 2}})
	if err := fs1.Commit(tx1); err != nil {
		t.Fatalf("Commit(tx1) failed: %v", err)
	}

	// Delete id=2 but rollback
	tx2, _ := fs1.Begin(false)
	pred := func(row sql.Row) (bool, error) {
		return row[0].I64 == 2, nil
	}
	if err := tx2.DeleteWhere("t", storage.RowPredicate(pred)); err != nil {
		t.Fatalf("DeleteWhere failed: %v", err)
	}
	if err := fs1.Rollback(tx2); err != nil {
		t.Fatalf("Rollback(tx2) failed: %v", err)
	}

	// Before restart (in-process) we might see id=1 only (no undo),
	// but after restart WAL-based recovery must ignore rolled-back deletes.
	fs2, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs2) failed: %v", err)
	}

	_, rows := scanAll(t, fs2, "t")
	if len(rows) != 2 {
		t.Fatalf("after restart: expected 2 rows (rollback of delete), got %d", len(rows))
	}
}
func TestFilestore_Recovery_Update_Replayed(t *testing.T) {
	dir := t.TempDir()

	fs1, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs1) failed: %v", err)
	}

	cols := []sql.Column{
		{Name: "id", Type: sql.TypeInt},
		{Name: "name", Type: sql.TypeString},
	}
	if err := fs1.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable(users) failed: %v", err)
	}

	tx1, _ := fs1.Begin(false)
	_ = tx1.Insert("users", sql.Row{
		{Type: sql.TypeInt, I64: 1},
		{Type: sql.TypeString, S: "Alice"},
	})
	if err := fs1.Commit(tx1); err != nil {
		t.Fatalf("Commit(tx1) failed: %v", err)
	}

	// Update name from Alice -> Bob and commit
	tx2, _ := fs1.Begin(false)
	pred := func(r sql.Row) (bool, error) {
		return r[0].I64 == 1, nil
	}
	updater := func(r sql.Row) (sql.Row, error) {
		r[1].S = "Bob"
		return r, nil
	}
	if err := tx2.UpdateWhere("users", storage.RowPredicate(pred), storage.RowUpdater(updater)); err != nil {
		t.Fatalf("UpdateWhere failed: %v", err)
	}
	if err := fs1.Commit(tx2); err != nil {
		t.Fatalf("Commit(tx2) failed: %v", err)
	}

	fs2, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs2) failed: %v", err)
	}

	_, rows := scanAll(t, fs2, "users")
	if len(rows) != 1 || rows[0][1].S != "Bob" {
		t.Fatalf("after restart: expected name=Bob, got rows=%v", rows)
	}
}
func TestFilestore_Recovery_Update_RollbackIgnored(t *testing.T) {
	dir := t.TempDir()

	fs1, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs1) failed: %v", err)
	}

	cols := []sql.Column{
		{Name: "id", Type: sql.TypeInt},
		{Name: "name", Type: sql.TypeString},
	}
	if err := fs1.CreateTable("users", cols); err != nil {
		t.Fatalf("CreateTable(users) failed: %v", err)
	}

	tx1, _ := fs1.Begin(false)
	_ = tx1.Insert("users", sql.Row{
		{Type: sql.TypeInt, I64: 1},
		{Type: sql.TypeString, S: "Alice"},
	})
	if err := fs1.Commit(tx1); err != nil {
		t.Fatalf("Commit(tx1) failed: %v", err)
	}

	// Update Alice -> Bob but rollback
	tx2, _ := fs1.Begin(false)
	pred := func(r sql.Row) (bool, error) {
		return r[0].I64 == 1, nil
	}
	updater := func(r sql.Row) (sql.Row, error) {
		r[1].S = "Bob"
		return r, nil
	}
	if err := tx2.UpdateWhere("users", storage.RowPredicate(pred), storage.RowUpdater(updater)); err != nil {
		t.Fatalf("UpdateWhere failed: %v", err)
	}
	if err := fs1.Rollback(tx2); err != nil {
		t.Fatalf("Rollback(tx2) failed: %v", err)
	}

	// After restart, WAL should ignore this rolled-back update, so we see Alice.
	fs2, err := New(dir)
	if err != nil {
		t.Fatalf("New(fs2) failed: %v", err)
	}

	_, rows := scanAll(t, fs2, "users")
	if len(rows) != 1 || rows[0][1].S != "Alice" {
		t.Fatalf("after restart: expected name=Alice, got rows=%v", rows)
	}
}
