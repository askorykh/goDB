# B-Tree index implementation

This package implements a simple on-disk B-Tree used by the storage layer for
single-column integer indexes.

## File layout

- Every index lives in its own file with magic header `BTREE1` followed by a
  root page ID and page count.
- Pages are 4KB and come in two flavors:
  - **Leaf pages (type 1):** sorted `[key, RID]` pairs. Each entry stores the
    indexed `int64` key plus the `(pageID, slotID)` of the row inside the
    filestore heap page.
  - **Internal pages (type 2):** child pointers interleaved with separator keys
    to guide navigation toward leaves.

See [`page.go`](page.go) for the exact header and slot encoding details used by
both page types.

## Manager and API

- `Manager` (in [`manager.go`](manager.go)) caches open indexes and materializes
  filenames using the `table_column.idx` convention inside the database
  directory.
- `Index` (defined in [`index.go`](index.go)) supports `Insert`, `Search`, and
  deletion operations. The current implementation focuses on inserts and lookups;
  delete paths are still marked TODO in `file.go`.

Index pages are split on insert when they run out of space, propagating new
separator keys upward and creating new roots as needed.
