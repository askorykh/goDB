# Filestore backend

The filestore backend is an experimental on-disk engine used by the REPL by default. It stores one `.godb` file per table plus a shared write-ahead log (`wal.log`) in the same directory.

## Table file layout

Each table file is a flat binary stream:

```
[header][rows...]

header:
  magic      : 5 bytes "GODB1"
  numCols    : uint16
  columns... : repeated numCols times
    nameLen  : uint16
    name     : nameLen bytes (UTF-8)
    type     : uint8 (matches `sql.DataType`)

rows:
  For every stored row:
    For every column:
      type tag : uint8 (`sql.DataType`, used to distinguish NULL)
      payload  : encoded by type
        INT    : int64 (little endian)
        FLOAT  : float64 (little endian)
        STRING : uint32 length + bytes
        BOOL   : 1 byte (0 or 1)
        NULL   : no payload
```

## WAL format

Durability is provided by a single append-only WAL (`wal.log`):

```
[magic "GODBWAL1"][records...]

record:
  recType      : uint8 (1=insert, 2=replace-all)
  tableNameLen : uint16
  tableName    : tableNameLen bytes
  rowCount     : uint32
  row payload  : rowCount encoded rows (same format as table files)
```

The WAL is fsynced on commit. Recovery is not implemented yet; the log is primarily a teaching aid showing how WAL append ordering works before on-disk redo/undo logic is added.

## Transaction semantics

- `BEGIN`/`COMMIT`/`ROLLBACK` are understood by the engine.
- With the filestore backend, rollbacks only cancel the in-memory engine transaction. Any writes already appended to the table files remain on disk because undo/redo recovery is not implemented yet.

## Tips for experimenting

- Data is written to the `./data` directory by default when running the REPL entrypoint.
- To reset the on-disk state, stop the REPL and remove the directory: `rm -rf ./data`.
