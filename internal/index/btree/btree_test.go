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

func TestLeafSplitMaintainsRIDPairs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx.idx")

	idxIface, err := OpenFileIndex(path, Meta{TableName: "t", Column: "id"})
	if err != nil {
		t.Fatalf("OpenFileIndex failed: %v", err)
	}
	idx := idxIface.(*fileIndex)
	defer idx.Close()

	// Fill a leaf to capacity to force a split.
	total := maxLeafKeys + 1
	for i := 0; i < total; i++ {
		rid := RID{PageID: uint32(i + 1), SlotID: uint16(i)}
		if err := idx.Insert(Key(i), rid); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	// Ensure searches return the matching RID after the split.
	checkKeys := []int{0, int(total / 2), total - 1}
	for _, k := range checkKeys {
		got, err := idx.Search(Key(k))
		if err != nil {
			t.Fatalf("Search %d failed: %v", k, err)
		}
		if len(got) != 1 {
			t.Fatalf("expected 1 RID for key %d, got %d", k, len(got))
		}
		want := RID{PageID: uint32(k + 1), SlotID: uint16(k)}
		if got[0] != want {
			t.Fatalf("RID mismatch for key %d: got %+v, want %+v", k, got[0], want)
		}
	}
}

func TestLeafSplitCreatesNewRootInternal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx.idx")

	idxIface, err := OpenFileIndex(path, Meta{TableName: "t", Column: "id"})
	if err != nil {
		t.Fatalf("OpenFileIndex failed: %v", err)
	}
	idx := idxIface.(*fileIndex)
	defer idx.Close()

	// Fill a leaf to capacity to force a split and create a new root internal node.
	total := maxLeafKeys + 1
	for i := 0; i < total; i++ {
		rid := RID{PageID: uint32(i + 1), SlotID: uint16(i)}
		if err := idx.Insert(Key(i), rid); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	rootPage, err := idx.readPage(idx.rootPageID)
	if err != nil {
		t.Fatalf("read root failed: %v", err)
	}
	rh := readPageHeader(rootPage)
	if rh.PageType != PageTypeInternal {
		t.Fatalf("root type = %d, want internal", rh.PageType)
	}
	if rh.NumKeys != 1 {
		t.Fatalf("root NumKeys = %d, want 1", rh.NumKeys)
	}

	children, keys, err := internalReadAll(rootPage, rh)
	if err != nil {
		t.Fatalf("internalReadAll failed: %v", err)
	}
	if len(children) != 2 {
		t.Fatalf("expected 2 children, got %d", len(children))
	}
	sep := keys[0]
	if sep != Key(total/2) {
		t.Fatalf("separator key = %d, want %d", sep, Key(total/2))
	}

	// Validate child leaf counts
	leftPage, err := idx.readPage(children[0])
	if err != nil {
		t.Fatalf("read left child failed: %v", err)
	}
	rightPage, err := idx.readPage(children[1])
	if err != nil {
		t.Fatalf("read right child failed: %v", err)
	}

	lh := readPageHeader(leftPage)
	rhh := readPageHeader(rightPage)
	if lh.NumKeys != uint32(total/2) {
		t.Fatalf("left leaf keys = %d, want %d", lh.NumKeys, total/2)
	}
	if rhh.NumKeys != uint32(total/2) {
		t.Fatalf("right leaf keys = %d, want %d", rhh.NumKeys, total/2)
	}
}

func TestInternalSplitGrowsTreeHeight(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "idx.idx")

	idxIface, err := OpenFileIndex(path, Meta{TableName: "t", Column: "id"})
	if err != nil {
		t.Fatalf("OpenFileIndex failed: %v", err)
	}
	idx := idxIface.(*fileIndex)
	defer idx.Close()

	// Insert enough keys to overflow the first internal root and force a higher-level split.
	total := (maxInternalKeys+1)*maxLeafKeys + 1
	for i := 0; i < total; i++ {
		rid := RID{PageID: uint32(i + 1), SlotID: uint16(i)}
		if err := idx.Insert(Key(i), rid); err != nil {
			t.Fatalf("Insert %d failed: %v", i, err)
		}
	}

	rootPage, err := idx.readPage(idx.rootPageID)
	if err != nil {
		t.Fatalf("read root failed: %v", err)
	}
	rh := readPageHeader(rootPage)
	if rh.PageType != PageTypeInternal {
		t.Fatalf("root type = %d, want internal", rh.PageType)
	}

	rootChildren, rootKeys, err := internalReadAll(rootPage, rh)
	if err != nil {
		t.Fatalf("internalReadAll failed: %v", err)
	}
	if len(rootChildren) < 2 {
		t.Fatalf("expected root to have at least 2 children after split, got %d", len(rootChildren))
	}
	if rh.NumKeys == 1 {
		// Ideal case: single promoted key with two children.
	} else if rh.NumKeys > uint32(maxInternalKeys) {
		t.Fatalf("root NumKeys = %d exceeds max %d", rh.NumKeys, maxInternalKeys)
	}

	// Children of the new root should be internal nodes (tree height = 3).
	leftChild, err := idx.readPage(rootChildren[0])
	if err != nil {
		t.Fatalf("read left child failed: %v", err)
	}
	rightChild, err := idx.readPage(rootChildren[1])
	if err != nil {
		t.Fatalf("read right child failed: %v", err)
	}

	if lh := readPageHeader(leftChild); lh.PageType != PageTypeInternal {
		t.Fatalf("left child type = %d, want internal", lh.PageType)
	}
	if rhh := readPageHeader(rightChild); rhh.PageType != PageTypeInternal {
		t.Fatalf("right child type = %d, want internal", rhh.PageType)
	}

	// The promoted separator should fall within the inserted key range.
	if sep := rootKeys[0]; sep <= 0 || sep >= Key(total-1) {
		t.Fatalf("separator key %d outside expected range", sep)
	}

	// Spot-check searches across the tree height.
	checkKeys := []int{0, total / 3, total - 1}
	for _, k := range checkKeys {
		got, err := idx.Search(Key(k))
		if err != nil {
			t.Fatalf("Search %d failed: %v", k, err)
		}
		if len(got) != 1 {
			t.Fatalf("expected 1 RID for key %d, got %d", k, len(got))
		}
		want := RID{PageID: uint32(k + 1), SlotID: uint16(k)}
		if got[0] != want {
			t.Fatalf("RID mismatch for key %d: got %+v, want %+v", k, got[0], want)
		}
	}
}
