# GoDB

GoDB is a tiny educational database engine written in Go.  
It started as a playground to learn Go and database internals: storage, SQL parsing, and a simple
query engine.

> ⚠️ This is a learning project, not production-ready software.

---

## Features (current)

- In-memory storage engine
- Simple SQL support:
    - `CREATE TABLE`
    - `INSERT INTO ... VALUES (...)`
    - `SELECT * FROM table`
    - `SELECT col1, col2 FROM table`
    - `SELECT ... FROM table WHERE column = literal`
    - `UPDATE table SET col = value WHERE column = literal`
    - `DELETE FROM table WHERE column = literal`
- REPL-style shell to run SQL commands

Supported data types:

- `INT`
- `FLOAT`
- `STRING`
- `BOOL`

---

## Getting Started

Clone the repository and run the REPL:

```bash
git clone https://github.com/askorykh/godb.git
cd godb
go run ./cmd/godb-server
```

## Roadmap (very rough)

- On-disk storage
- Better query planner / optimizer
- Indexes
- Transactions at SQL level (BEGIN / COMMIT / ROLLBACK)
- Maybe: distributed experiments later
