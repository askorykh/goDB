package btree

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
)

const (
	fileHeaderSize = len(indexFileMagic) + 8 // "BTREE1" + root + pageCount

	leafEntrySize     = 16 // 8 bytes key + 8 bytes RID
	internalEntrySize = 12 // child(4) + key(8)

	maxLeafKeys     = (PageSize - 16) / leafEntrySize
	maxInternalKeys = (PageSize - 16 - 4) / internalEntrySize // 4 bytes for initial child0
)

type fileIndex struct {
	f          *os.File
	meta       Meta
	rootPageID uint32
	pageCount  uint32
}

// Insert implements Index.Insert for fileIndex (without splits yet).
func (idx *fileIndex) Insert(key Key, rid RID) error {
	leafID, leafPage, path, err := idx.findLeafForKeyWithPath(key)
	if err != nil {
		return err
	}

	h := readPageHeader(leafPage)
	if h.PageType != PageTypeLeaf {
		return fmt.Errorf("btree: Insert: expected leaf, got type %d", h.PageType)
	}
	n := h.NumKeys

	// Fast path: leaf has room
	if n < uint32(maxLeafKeys) {
		// Simple sorted insert (linear search + shift)
		var pos uint32
		for pos = 0; pos < n; pos++ {
			k := leafGetKey(leafPage, pos)
			if key < k {
				break
			}
		}

		if pos < n {
			moveStart := 16 + int(pos)*leafEntrySize
			moveBytes := int(n-pos) * leafEntrySize
			copy(leafPage[moveStart+leafEntrySize:moveStart+leafEntrySize+moveBytes],
				leafPage[moveStart:moveStart+moveBytes])
		}

		leafSetEntry(leafPage, pos, key, rid)
		h.NumKeys = n + 1
		writePageHeader(leafPage, h)
		return idx.writePage(leafID, leafPage)
	}

	// Leaf is full → split.
	keys, rids := leafReadAll(leafPage, h)

	// Add new entry
	keys = append(keys, key)
	rids = append(rids, rid)

	// Sort by key (stable so duplicates maintain insert order)
	sort.SliceStable(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	// But we must keep rids paired with keys. So better build a slice of pairs.
	// Build entries before sort to keep keys and RIDs together.
	type entry struct {
		k Key
		r RID
	}
	entries := make([]entry, len(keys))
	for i := range keys {
		entries[i] = entry{k: keys[i], r: rids[i]}
	}
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].k < entries[j].k
	})

	// Compute split point
	total := len(entries)
	split := total / 2

	leftEntries := entries[:split]
	rightEntries := entries[split:]

	// Overwrite left (existing leaf)
	leftKeys := make([]Key, len(leftEntries))
	leftRIDs := make([]RID, len(leftEntries))
	for i, e := range leftEntries {
		leftKeys[i] = e.k
		leftRIDs[i] = e.r
	}
	leafWriteAll(leafPage, leftKeys, leftRIDs)
	if err := idx.writePage(leafID, leafPage); err != nil {
		return err
	}

	// Create right leaf
	rightID, rightPage, err := idx.allocPage(PageTypeLeaf)
	if err != nil {
		return err
	}
	rightKeys := make([]Key, len(rightEntries))
	rightRIDs := make([]RID, len(rightEntries))
	for i, e := range rightEntries {
		rightKeys[i] = e.k
		rightRIDs[i] = e.r
	}
	leafWriteAll(rightPage, rightKeys, rightRIDs)
	if err := idx.writePage(rightID, rightPage); err != nil {
		return err
	}

	// Separator key is first key of right leaf
	sepKey := rightKeys[0]

	// Insert separator into parent (may create new root).
	if err := idx.insertIntoParent(leafID, rightID, sepKey, path); err != nil {
		return err
	}

	return nil
}

func (idx *fileIndex) Delete(key Key, rid RID) error {
	// We'll implement proper delete later.
	// For now, just return not implemented so it compiles.
	return fmt.Errorf("btree: Delete not implemented yet")
}

func (idx *fileIndex) DeleteKey(key Key) error {
	// Also to be implemented later.
	return fmt.Errorf("btree: DeleteKey not implemented yet")
}

// Search implements Index.Search: return all RIDs for a given key.
func (idx *fileIndex) Search(key Key) ([]RID, error) {
	_, p, err := idx.findLeafForKey(key)
	if err != nil {
		return nil, err
	}

	h := readPageHeader(p)
	if h.PageType != PageTypeLeaf {
		return nil, fmt.Errorf("btree: Search: expected leaf, got type %d", h.PageType)
	}
	n := h.NumKeys

	// Binary search for first position >= key
	lo, hi := uint32(0), n
	for lo < hi {
		mid := (lo + hi) / 2
		k := leafGetKey(p, mid)
		if key > k {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	// Collect all equal keys from lo onwards
	var rids []RID
	for i := lo; i < n; i++ {
		k := leafGetKey(p, i)
		if k != key {
			break
		}
		rids = append(rids, leafGetRID(p, i))
	}
	return rids, nil
}

func (idx *fileIndex) Close() error {
	if idx.f != nil {
		err := idx.f.Close()
		idx.f = nil
		return err
	}
	return nil
}

// File header layout:
// [magic 6 bytes][rootPageID 4][pageCount 4]
// total = 14 bytes
func writeFileHeader(f *os.File, root, pages uint32) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	if _, err := f.Write([]byte(indexFileMagic)); err != nil {
		return err
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[0:4], root)
	binary.LittleEndian.PutUint32(buf[4:8], pages)

	_, err := f.Write(buf)
	return err
}

func readFileHeader(f *os.File) (root uint32, pages uint32, err error) {
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return
	}

	magic := make([]byte, 6)
	if _, err = io.ReadFull(f, magic); err != nil {
		return
	}
	if string(magic) != indexFileMagic {
		err = fmt.Errorf("btree: bad index magic")
		return
	}

	buf := make([]byte, 8)
	if _, err = io.ReadFull(f, buf); err != nil {
		return
	}
	root = binary.LittleEndian.Uint32(buf[0:4])
	pages = binary.LittleEndian.Uint32(buf[4:8])
	return
}
func OpenFileIndex(path string, meta Meta) (Index, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	idx := &fileIndex{
		f:    f,
		meta: meta,
	}

	if fi.Size() == 0 {
		// brand new index: create a single leaf root
		rootPage := make([]byte, PageSize)
		h := PageHeader{
			PageType:     PageTypeLeaf,
			ParentPageID: 0,
			NumKeys:      0,
		}
		writePageHeader(rootPage, h)

		// Write header + first leaf page
		if err := writeFileHeader(f, 0, 1); err != nil {
			return nil, err
		}
		if _, err := f.Write(rootPage); err != nil {
			return nil, err
		}

		idx.rootPageID = 0
		idx.pageCount = 1
		return idx, nil
	}

	// Existing index → read header
	root, pages, err := readFileHeader(f)
	if err != nil {
		return nil, err
	}
	idx.rootPageID = root
	idx.pageCount = pages
	return idx, nil
}
func (idx *fileIndex) pageOffset(pageID uint32) int64 {
	return int64(fileHeaderSize) + int64(pageID)*PageSize
}
func (idx *fileIndex) readPage(pageID uint32) ([]byte, error) {
	p := make([]byte, PageSize)
	off := idx.pageOffset(pageID)
	if _, err := idx.f.ReadAt(p, off); err != nil {
		return nil, fmt.Errorf("btree: read page %d: %w", pageID, err)
	}
	return p, nil
}
func (idx *fileIndex) writePage(pageID uint32, p []byte) error {
	if len(p) != PageSize {
		return fmt.Errorf("btree: writePage: wrong page size %d", len(p))
	}
	off := idx.pageOffset(pageID)
	if _, err := idx.f.WriteAt(p, off); err != nil {
		return fmt.Errorf("btree: write page %d: %w", pageID, err)
	}
	return nil
}

// findLeafForKey walks from the root down to the leaf where `key` belongs.
// It returns (pageID, pageBytes).
func (idx *fileIndex) findLeafForKey(key Key) (uint32, []byte, error) {
	pageID := idx.rootPageID

	for {
		p, err := idx.readPage(pageID)
		if err != nil {
			return 0, nil, err
		}
		h := readPageHeader(p)

		switch h.PageType {
		case PageTypeLeaf:
			return pageID, p, nil

		case PageTypeInternal:
			n := h.NumKeys
			if n == 0 {
				return 0, nil, fmt.Errorf("btree: empty internal node at page %d", pageID)
			}

			// Choose child:
			// if key < key0 -> child0
			// else find largest i with key >= key_i -> child_{i+1}
			var childIdx uint32
			var i uint32
			for i = 0; i < n; i++ {
				k := internalGetKey(p, i)
				if key < k {
					childIdx = i
					break
				}
			}
			if i == n {
				// key >= all keys -> rightmost child
				childIdx = n
			}

			childPageID := internalGetChild(p, childIdx)
			pageID = childPageID

		default:
			return 0, nil, fmt.Errorf("btree: unknown page type %d at page %d", h.PageType, pageID)
		}
	}
}
func (idx *fileIndex) allocPage(pageType uint8) (uint32, []byte, error) {
	pageID := idx.pageCount
	idx.pageCount++

	p := make([]byte, PageSize)
	h := PageHeader{
		PageType:     pageType,
		ParentPageID: 0, // we won't rely on this yet
		NumKeys:      0,
	}
	writePageHeader(p, h)

	// Write page to disk
	if err := idx.writePage(pageID, p); err != nil {
		return 0, nil, err
	}

	// Update file header (rootPageID unchanged)
	if err := writeFileHeader(idx.f, idx.rootPageID, idx.pageCount); err != nil {
		return 0, nil, err
	}

	return pageID, p, nil
}
func leafReadAll(p []byte, h PageHeader) ([]Key, []RID) {
	n := h.NumKeys
	keys := make([]Key, n)
	rids := make([]RID, n)
	for i := uint32(0); i < n; i++ {
		keys[i] = leafGetKey(p, i)
		rids[i] = leafGetRID(p, i)
	}
	return keys, rids
}

func leafWriteAll(p []byte, keys []Key, rids []RID) {
	if len(keys) != len(rids) {
		panic("leafWriteAll: keys and rids length mismatch")
	}
	h := PageHeader{
		PageType:     PageTypeLeaf,
		ParentPageID: 0, // we ignore for now
		NumKeys:      uint32(len(keys)),
	}
	writePageHeader(p, h)

	for i := range keys {
		leafSetEntry(p, uint32(i), keys[i], rids[i])
	}
}
func internalReadAll(p []byte, h PageHeader) ([]uint32, []Key, error) {
	n := h.NumKeys
	children := make([]uint32, n+1)
	keys := make([]Key, n)

	off := 16
	if len(p) < off+4 {
		return nil, nil, fmt.Errorf("btree: corrupt internal page header area")
	}

	// child0
	children[0] = binary.LittleEndian.Uint32(p[off : off+4])
	off += 4

	for i := uint32(0); i < n; i++ {
		if len(p) < off+8+4 {
			return nil, nil, fmt.Errorf("btree: corrupt internal page")
		}
		k := int64(binary.LittleEndian.Uint64(p[off : off+8]))
		off += 8
		keys[i] = Key(k)

		children[i+1] = binary.LittleEndian.Uint32(p[off : off+4])
		off += 4
	}

	return children, keys, nil
}

func internalWriteAll(p []byte, h PageHeader, children []uint32, keys []Key) error {
	n := h.NumKeys
	if uint32(len(keys)) != n {
		return fmt.Errorf("btree: internalWriteAll: keys length mismatch")
	}
	if len(children) != int(n)+1 {
		return fmt.Errorf("btree: internalWriteAll: children length mismatch")
	}

	writePageHeader(p, h)

	off := 16
	binary.LittleEndian.PutUint32(p[off:off+4], children[0])
	off += 4

	for i := uint32(0); i < n; i++ {
		binary.LittleEndian.PutUint64(p[off:off+8], uint64(keys[i]))
		off += 8
		binary.LittleEndian.PutUint32(p[off:off+4], children[i+1])
		off += 4
	}

	return nil
}

// findLeafForKeyWithPath walks from root to leaf and returns
// (leafPageID, leafPageBytes, pathOfPageIDs), where path[len-1] = leaf.
func (idx *fileIndex) findLeafForKeyWithPath(key Key) (uint32, []byte, []uint32, error) {
	pageID := idx.rootPageID
	var path []uint32

	for {
		path = append(path, pageID)

		p, err := idx.readPage(pageID)
		if err != nil {
			return 0, nil, nil, err
		}
		h := readPageHeader(p)

		switch h.PageType {
		case PageTypeLeaf:
			return pageID, p, path, nil

		case PageTypeInternal:
			n := h.NumKeys
			if n == 0 {
				return 0, nil, nil, fmt.Errorf("btree: empty internal node at page %d", pageID)
			}

			children, keys, err := internalReadAll(p, h)
			if err != nil {
				return 0, nil, nil, err
			}

			// Decide which child to follow
			var childIdx int
			i := 0
			for i = 0; i < int(n); i++ {
				if key < keys[i] {
					break
				}
			}
			childIdx = i // i in [0..n], if i==n: rightmost

			pageID = children[childIdx]

		default:
			return 0, nil, nil, fmt.Errorf("btree: unknown page type %d at page %d", h.PageType, pageID)
		}
	}
}

// insertIntoParent is called after splitting a leaf:
// leftID: old leaf page
// rightID: new leaf page
// sepKey: first key of right leaf
// path: path from root to leftID (leaf is last element).
func (idx *fileIndex) insertIntoParent(leftID, rightID uint32, sepKey Key, path []uint32) error {
	// If left was root, create a new root internal.
	if len(path) == 1 {
		// New root internal with two children and one key.
		rootID, rootPage, err := idx.allocPage(PageTypeInternal)
		if err != nil {
			return err
		}

		h := PageHeader{
			PageType:     PageTypeInternal,
			ParentPageID: 0,
			NumKeys:      1,
		}
		children := []uint32{leftID, rightID}
		keys := []Key{sepKey}

		if err := internalWriteAll(rootPage, h, children, keys); err != nil {
			return err
		}
		if err := idx.writePage(rootID, rootPage); err != nil {
			return err
		}

		// Update in-memory and on-disk header
		idx.rootPageID = rootID
		if err := writeFileHeader(idx.f, idx.rootPageID, idx.pageCount); err != nil {
			return err
		}

		return nil
	}

	// Non-root: parent is the second-to-last pageID in path.
	parentID := path[len(path)-2]
	parentPage, err := idx.readPage(parentID)
	if err != nil {
		return err
	}
	hp := readPageHeader(parentPage)
	if hp.PageType != PageTypeInternal {
		return fmt.Errorf("btree: parent of leaf is not internal (page %d)", parentID)
	}

	children, keys, err := internalReadAll(parentPage, hp)
	if err != nil {
		return err
	}

	// Find position of leftID in children
	var pos int = -1
	for i, c := range children {
		if c == leftID {
			pos = i
			break
		}
	}
	if pos == -1 {
		return fmt.Errorf("btree: parent does not reference left child %d", leftID)
	}

	if hp.NumKeys >= uint32(maxInternalKeys) {
		return fmt.Errorf("btree: internal node %d is full (internal splits not implemented yet)", parentID)
	}

	// Insert sepKey at keys[pos], and rightID at children[pos+1].
	// children: len = n+1, keys: len = n
	n := int(hp.NumKeys)

	// Insert child at pos+1
	children = append(children, 0) // grow by 1
	copy(children[pos+2:], children[pos+1:])
	children[pos+1] = rightID

	// Insert key at pos
	keys = append(keys, 0)
	copy(keys[pos+1:], keys[pos:])
	keys[pos] = sepKey

	hp.NumKeys = uint32(n + 1)
	if err := internalWriteAll(parentPage, hp, children, keys); err != nil {
		return err
	}
	if err := idx.writePage(parentID, parentPage); err != nil {
		return err
	}

	return nil
}
