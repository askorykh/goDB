package filestore

import (
	"encoding/binary"
	"fmt"
	"goDB/internal/sql"
)

const (
	PageSize = 4096

	pageMagic = "GPG1" // GoDB Page v1

	pageTypeHeap uint8 = 1
)

// Page header layout (on disk):
//
// offset  size  field
// 0       4     magic "GPG1"
// 4       4     pageID (uint32)
// 8       1     pageType (1 = heap)
// 9       1     reserved
// 10      2     numSlots (uint16)
// 12      2     freeStart (uint16) - where next row bytes can be written
// 14      2     reserved
// 16..    row area...
//
// Slot directory is at the end of the page, each slot 4 bytes:
//   [offset uint16][length uint16]
//
// Invariants:
//   freeStart <= PageSize - numSlots*4
//   slot i is located at: PageSize - (i+1)*4
//   deleted slot: offset == 0xFFFF
//

// pageBuf is a 4KB page in memory.
type pageBuf []byte

// newEmptyHeapPage initializes a new heap page with given pageID.
func newEmptyHeapPage(pageID uint32) pageBuf {
	buf := make([]byte, PageSize)
	// magic
	copy(buf[0:4], []byte(pageMagic))
	// pageID
	binary.LittleEndian.PutUint32(buf[4:8], pageID)
	// pageType
	buf[8] = pageTypeHeap
	// numSlots = 0
	binary.LittleEndian.PutUint16(buf[10:12], 0)
	// freeStart = header end (16)
	binary.LittleEndian.PutUint16(buf[12:14], 16)
	return buf
}

func (p pageBuf) pageID() uint32 {
	return binary.LittleEndian.Uint32(p[4:8])
}

func (p pageBuf) numSlots() uint16 {
	return binary.LittleEndian.Uint16(p[10:12])
}

func (p pageBuf) setNumSlots(n uint16) {
	binary.LittleEndian.PutUint16(p[10:12], n)
}

func (p pageBuf) freeStart() uint16 {
	return binary.LittleEndian.Uint16(p[12:14])
}

func (p pageBuf) setFreeStart(off uint16) {
	binary.LittleEndian.PutUint16(p[12:14], off)
}

// slotPos returns the byte index in the page of slot i (0-based).
func slotPos(i uint16) int {
	return PageSize - int(i+1)*4
}

// getSlot reads slot i (0-based): (offset, length).
func (p pageBuf) getSlot(i uint16) (uint16, uint16) {
	pos := slotPos(i)
	off := binary.LittleEndian.Uint16(p[pos : pos+2])
	length := binary.LittleEndian.Uint16(p[pos+2 : pos+4])
	return off, length
}

// setSlot writes slot i (0-based).
func (p pageBuf) setSlot(i uint16, off, length uint16) {
	pos := slotPos(i)
	binary.LittleEndian.PutUint16(p[pos:pos+2], off)
	binary.LittleEndian.PutUint16(p[pos+2:pos+4], length)
}

// insertRow tries to place an encoded row into the page.
// Returns (slotIndex, error). If there's not enough space, returns error.
func (p pageBuf) insertRow(rowBytes []byte) (uint16, error) {
	nSlots := p.numSlots()
	freeStart := p.freeStart()

	rowLen := uint16(len(rowBytes))

	// Check if we have a deleted slot we can reuse.
	var reuseSlot *uint16
	for i := uint16(0); i < nSlots; i++ {
		off, length := p.getSlot(i)
		if off == 0xFFFF && length == 0 {
			reuseSlot = &i
			break
		}
	}

	neededForRow := int(rowLen)
	neededForNewSlot := 4 // each slot: offset uint16 + length uint16

	// Compute how much space we need in total
	needed := neededForRow
	if reuseSlot == nil {
		needed += neededForNewSlot
	}

	// Current free end = start of slot directory
	freeEnd := PageSize - int(nSlots)*4

	if int(freeStart)+needed > freeEnd {
		return 0, fmt.Errorf("page: not enough free space")
	}

	// Write row bytes at freeStart
	copy(p[freeStart:int(freeStart)+len(rowBytes)], rowBytes)

	var slotIdx uint16
	if reuseSlot != nil {
		slotIdx = *reuseSlot
	} else {
		slotIdx = nSlots
		p.setNumSlots(nSlots + 1)
	}

	// Point slot to row
	p.setSlot(slotIdx, freeStart, rowLen)
	p.setFreeStart(freeStart + rowLen)

	return slotIdx, nil
}

// iterateRows calls fn(slotIndex, row) for each non-deleted row in order.
func (p pageBuf) iterateRows(numCols int, fn func(slot uint16, row sql.Row) error) error {
	nSlots := p.numSlots()
	for i := uint16(0); i < nSlots; i++ {
		off, length := p.getSlot(i)
		if off == 0xFFFF || length == 0 {
			// deleted / empty slot
			continue
		}
		start := int(off)
		end := int(off) + int(length)
		if end > len(p) {
			return fmt.Errorf("page: corrupt slot %d", i)
		}
		rowBytes := p[start:end]
		// decode rowBytes using readRowFromBytes (we'll add this helper)
		row, err := readRowFromBytes(rowBytes, numCols)
		if err != nil {
			return fmt.Errorf("page: read row at slot %d: %w", i, err)
		}
		if err := fn(i, row); err != nil {
			return err
		}
	}
	return nil
}

func (p pageBuf) deleteSlot(i uint16) {
	// Capture existing offset/length so we can reclaim trailing space if possible.
	off, length := p.getSlot(i)

	// Mark as deleted. We use 0xFFFF/0 as the “tombstone” value.
	p.setSlot(i, 0xFFFF, 0)

	// If this row occupied the contiguous end of the in-use area, rewind freeStart
	// to reclaim space. We walk backwards through rows that end at the current
	// freeStart so consecutive deletions reclaim space in order of most recent
	// inserts.
	freeStart := p.freeStart()
	if off != 0xFFFF && length != 0 {
		if end := off + length; end == freeStart {
			newFreeStart := off
			for {
				progressed := false
				for idx := uint16(0); idx < p.numSlots(); idx++ {
					o, l := p.getSlot(idx)
					if o == 0xFFFF || l == 0 {
						continue
					}
					if o+l == newFreeStart {
						newFreeStart = o
						progressed = true
					}
				}
				if !progressed {
					break
				}
			}
			p.setFreeStart(newFreeStart)
		}
	}

	// Shrink slot directory by dropping tombstones at the end. This allows
	// future inserts to reclaim the slot-directory space in addition to row data.
	nSlots := p.numSlots()
	for nSlots > 0 {
		lastIdx := nSlots - 1
		o, l := p.getSlot(lastIdx)
		if o == 0xFFFF && l == 0 {
			nSlots--
			p.setNumSlots(nSlots)
			continue
		}
		break
	}
}
