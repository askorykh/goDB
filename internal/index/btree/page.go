package btree

import (
	"encoding/binary"
	"errors"
)

const (
	PageSize = 4096

	PageTypeLeaf     = 1
	PageTypeInternal = 2

	indexFileMagic = "BTREE1" // 6 bytes
)

var (
	ErrBadPage = errors.New("btree: bad page")
)

// PageHeader describes the fixed part of an index page.
type PageHeader struct {
	PageType     uint8
	ParentPageID uint32
	NumKeys      uint32
}

func readPageHeader(p []byte) PageHeader {
	return PageHeader{
		PageType:     p[0],
		ParentPageID: binary.LittleEndian.Uint32(p[4:8]),
		NumKeys:      binary.LittleEndian.Uint32(p[8:12]),
	}
}

func writePageHeader(p []byte, h PageHeader) {
	p[0] = h.PageType
	// p[1:4] unused
	binary.LittleEndian.PutUint32(p[4:8], h.ParentPageID)
	binary.LittleEndian.PutUint32(p[8:12], h.NumKeys)
}

func leafGetKey(p []byte, idx uint32) Key {
	off := 16 + int(idx)*leafEntrySize // skip header (16 bytes)
	return int64(binary.LittleEndian.Uint64(p[off : off+8]))
}

func leafGetRID(p []byte, idx uint32) RID {
	off := 16 + int(idx)*leafEntrySize + 8
	pageID := binary.LittleEndian.Uint32(p[off : off+4])
	slotID := binary.LittleEndian.Uint16(p[off+4 : off+6])
	return RID{PageID: pageID, SlotID: slotID}
}

func leafSetEntry(p []byte, idx uint32, key Key, rid RID) {
	off := 16 + int(idx)*leafEntrySize
	binary.LittleEndian.PutUint64(p[off:off+8], uint64(key))
	off += 8
	binary.LittleEndian.PutUint32(p[off:off+4], rid.PageID)
	binary.LittleEndian.PutUint16(p[off+4:off+6], rid.SlotID)
}

func internalGetChild(p []byte, idx uint32) uint32 {
	off := 16 + int(idx)*internalEntrySize
	return binary.LittleEndian.Uint32(p[off : off+4])
}

func internalGetKey(p []byte, idx uint32) Key {
	off := 16 + int(idx)*internalEntrySize + 4
	return int64(binary.LittleEndian.Uint64(p[off : off+8]))
}

func internalSetEntry(p []byte, idx uint32, child uint32, key Key) {
	off := 16 + int(idx)*internalEntrySize
	binary.LittleEndian.PutUint32(p[off:off+4], child)
	binary.LittleEndian.PutUint64(p[off+4:off+12], uint64(key))
}
