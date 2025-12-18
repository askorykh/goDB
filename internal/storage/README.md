# Storage layer overview

This package defines the storage interfaces used by the engine plus the concrete
implementations that back the SQL executor.

## Interfaces

- `Engine` exposes `Begin`/`Commit`/`Rollback` plus helpers for creating tables
  and discovering schemas. It is the boundary the execution engine talks to.
- `Tx` operations cover table scans, inserts, delete/update helpers, and a
  full-table `ReplaceAll` used by the SQL UPDATE/DELETE implementations.
- `RowPredicate` and `RowUpdater` callbacks power the row-level filtering and
  rewrite logic used by the memstore backend.

See [`storage.go`](storage.go) for the exact signatures and comments.

## Implementation

[`memstore`](memstore) is an in-memory reference engine used by tests and to
keep the code paths simple when persistence is not required.
