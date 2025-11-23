package filestore

import (
	"bytes"
	"goDB/internal/sql"
	"testing"
)

// helper: encode a row into []byte using the same format as writeRow/readRow.
func encodeRow(t *testing.T, row sql.Row) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := writeRow(&buf, row); err != nil {
		t.Fatalf("writeRow failed: %v", err)
	}
	return buf.Bytes()
}

func TestPage_InsertAndIterateRows(t *testing.T) {
	// simple schema: 3 columns
	numCols := 3

	p := newEmptyHeapPage(1)

	row1 := sql.Row{
		{Type: sql.TypeInt, I64: 1},
		{Type: sql.TypeString, S: "Alice"},
		{Type: sql.TypeBool, B: true},
	}
	row2 := sql.Row{
		{Type: sql.TypeInt, I64: 2},
		{Type: sql.TypeString, S: "Bob"},
		{Type: sql.TypeBool, B: false},
	}

	// insert two rows
	slot0, err := p.insertRow(encodeRow(t, row1))
	if err != nil {
		t.Fatalf("insertRow(row1) failed: %v", err)
	}
	if slot0 != 0 {
		t.Fatalf("expected first slot index 0, got %d", slot0)
	}

	slot1, err := p.insertRow(encodeRow(t, row2))
	if err != nil {
		t.Fatalf("insertRow(row2) failed: %v", err)
	}
	if slot1 != 1 {
		t.Fatalf("expected second slot index 1, got %d", slot1)
	}

	if p.numSlots() != 2 {
		t.Fatalf("expected numSlots=2, got %d", p.numSlots())
	}

	// iterate and collect rows
	var got []sql.Row
	err = p.iterateRows(numCols, func(slot uint16, r sql.Row) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("iterateRows failed: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}

	// verify content and order
	if got[0][0].I64 != 1 || got[0][1].S != "Alice" || !got[0][2].B {
		t.Fatalf("unexpected first row: %+v", got[0])
	}
	if got[1][0].I64 != 2 || got[1][1].S != "Bob" || got[1][2].B {
		t.Fatalf("unexpected second row: %+v", got[1])
	}
}

func TestPage_NotEnoughSpace(t *testing.T) {
	p := newEmptyHeapPage(1)

	// Make a big string so that we can almost fill the page.
	// We don't need exact numbers, just something large.
	largeStr := make([]byte, 3000)
	for i := range largeStr {
		largeStr[i] = 'x'
	}

	row := sql.Row{
		{Type: sql.TypeInt, I64: 1},
		{Type: sql.TypeString, S: string(largeStr)},
	}

	// Insert until we run out of space.
	var count int
	for {
		_, err := p.insertRow(encodeRow(t, row))
		if err != nil {
			// we expect to hit "not enough free space" eventually
			break
		}
		count++
	}

	if count == 0 {
		t.Fatalf("expected at least one row to fit into the page, got 0")
	}
}

func TestPage_DeletedSlotIsSkipped(t *testing.T) {
	numCols := 2
	p := newEmptyHeapPage(1)

	row1 := sql.Row{
		{Type: sql.TypeInt, I64: 1},
		{Type: sql.TypeString, S: "Alice"},
	}
	row2 := sql.Row{
		{Type: sql.TypeInt, I64: 2},
		{Type: sql.TypeString, S: "Bob"},
	}

	// Insert two rows -> slots 0 and 1
	if _, err := p.insertRow(encodeRow(t, row1)); err != nil {
		t.Fatalf("insertRow(row1) failed: %v", err)
	}
	if _, err := p.insertRow(encodeRow(t, row2)); err != nil {
		t.Fatalf("insertRow(row2) failed: %v", err)
	}

	// Simulate deletion of slot 0 by marking it as deleted.
	p.setSlot(0, 0xFFFF, 0)

	var got []sql.Row
	err := p.iterateRows(numCols, func(slot uint16, r sql.Row) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("iterateRows failed: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("expected 1 visible row after deletion, got %d", len(got))
	}
	if got[0][0].I64 != 2 || got[0][1].S != "Bob" {
		t.Fatalf("unexpected remaining row: %+v", got[0])
	}
}
