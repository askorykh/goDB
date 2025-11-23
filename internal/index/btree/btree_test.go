package btree

import (
	"path/filepath"
	"testing"
)

func TestLeafInsertAndSearch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx.idx")

	idxIface, err := OpenFileIndex(path, Meta{TableName: "t", Column: "id"})
	if err != nil {
		t.Fatalf("OpenFileIndex failed: %v", err)
	}
	idx := idxIface.(*fileIndex)
	defer idx.Close()

	rid := RID{PageID: 1, SlotID: 10}
	if err := idx.Insert(42, rid); err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	got, err := idx.Search(42)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 RID, got %d", len(got))
	}
	if got[0] != rid {
		t.Fatalf("RID mismatch: got %+v, want %+v", got[0], rid)
	}
}

func TestLeafInsertOrderAndDuplicates(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx.idx")

	idxIface, err := OpenFileIndex(path, Meta{TableName: "t", Column: "id"})
	if err != nil {
		t.Fatalf("OpenFileIndex failed: %v", err)
	}
	idx := idxIface.(*fileIndex)
	defer idx.Close()

	// Insert out of order + duplicates
	rids := []RID{
		{PageID: 1, SlotID: 1},
		{PageID: 1, SlotID: 2},
		{PageID: 1, SlotID: 3},
	}
	_ = idx.Insert(50, rids[0])
	_ = idx.Insert(10, rids[1])
	_ = idx.Insert(50, rids[2])

	// Check duplicates for 50
	got, err := idx.Search(50)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 RIDs for key 50, got %d", len(got))
	}
}
