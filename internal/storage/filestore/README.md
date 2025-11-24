# Filestore backend

The filestore backend is the default on-disk engine used by the REPL. It stores
one `.godb` file per table plus a shared write-ahead log (`wal.log`) in the same
directory.

## Table file layout

Each table file is a binary stream composed of a schema header followed by 4KB
heap pages:

```
[header][pages...]

header:
  magic      : 5 bytes "GODB1"
  numCols    : uint16
  columns... : repeated numCols times
    nameLen  : uint16
    name     : nameLen bytes (UTF-8)
    type     : uint8 (matches `sql.DataType`)

page (4096 bytes):
  magic     : 4 bytes "GPG1"
  pageID    : uint32
  pageType  : uint8 (1 = heap)
  numSlots  : uint16
  freeStart : uint16 (offset where the next row bytes can be written)
  row area  : variable
  slot dir  : grows backward from the end of the page
    slot i: [offset uint16][length uint16]; deleted slots use offset 0xFFFF

rows inside a page:
  encoded payload for each column (same format as the header types):
    INT    : int64 (little endian)
    FLOAT  : float64 (little endian)
    STRING : uint32 length + bytes
    BOOL   : 1 byte (0 or 1)
    NULL   : no payload
```

## WAL format

Durability is provided by a single append-only WAL (`wal.log`). The current
version uses the magic prefix `GODBWAL2` and encodes records as:

```
[magic "GODBWAL2"][records...]

record header:
  recType : uint8
  txID    : uint64
  payload : varies by type

record types:
  1 = BEGIN      (no payload)
  2 = COMMIT     (no payload)
  3 = ROLLBACK   (no payload)
  4 = INSERT     (payload: tableNameLen uint16, tableName bytes,
                           rowCount uint32 = 1, encoded row)
  5 = REPLACEALL (payload: tableNameLen uint16, tableName bytes,
                           rowCount uint32, repeated encoded rows)
  6 = DELETE     (payload: tableNameLen uint16, tableName bytes,
                           rowCount uint32 = 1, encoded deleted row)
  7 = UPDATE     (payload: tableNameLen uint16, tableName bytes,
                           rowCount uint32 = 2, encoded [oldRow, newRow])
```

WAL writes are fsynced on `COMMIT` and `ROLLBACK`. Table pages are updated
before commit, so redo-only recovery depends on WAL entries to rebuild state
after a crash.

## Recovery process

On startup the engine replays the WAL to rebuild durable table contents:

1. Load the header/schema for every existing table file.
2. Truncate each table back to just its header (clearing all pages).
3. Parse `wal.log` into per-transaction op lists, tracking `COMMIT`/`ROLLBACK`.
4. Replay committed, non-rolled-back transactions in log order into an
   in-memory row list per table applying `INSERT`, `REPLACEALL`, `DELETE`, and
   `UPDATE` semantics.
5. Write the rebuilt rows back out via `ReplaceAll`, regenerating heap pages.

Uncommitted or rolled-back transactions are ignored during replay so their
changes do not survive recovery.

## Transaction semantics

- `BEGIN`/`COMMIT`/`ROLLBACK` are understood by the engine and logged in the
  WAL. `COMMIT` fsyncs the WAL to ensure durability of prior writes.
- Mutations (`INSERT`, `UPDATE`, `DELETE`) update table files immediately;
  `ROLLBACK` does not undo those on-disk changes until a restart, when recovery
  filters out rolled-back transactions while rebuilding from the WAL.
- `REPLACEALL` is used by engine-level UPDATE/DELETE implementations to rewrite
  whole tables and is fully logged for recovery.

## Tips for experimenting

- Data is written to the `./data` directory by default when running the REPL
  entrypoint.
- To reset the on-disk state, stop the REPL and remove the directory:
  `rm -rf ./data`.
