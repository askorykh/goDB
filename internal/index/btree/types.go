package btree

// RID identifies a row in a heap page (table file).
// pageID is the heap page number, slotID is the row slot within that page.
type RID struct {
	PageID uint32
	SlotID uint16
}
