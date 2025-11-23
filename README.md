# GoDB

GoDB is a tiny educational database engine written in Go. It exists as a playground to explore storage internals, SQL parsing, and a minimal query engine.

> ⚠️ This is a learning project, not production-ready software.

## Features

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
- Supported data types: `INT`, `FLOAT`, `STRING`, `BOOL`

## Requirements

- [Go](https://go.dev/) 1.25+

## Getting started

```bash
# Clone and enter the project
git clone https://github.com/askorykh/godb.git
cd godb

# Run the REPL server
go run ./cmd/godb-server
```

While in the REPL, try commands such as:

```sql
CREATE TABLE users (id INT, name STRING, active BOOL);
INSERT INTO users VALUES (1, 'Alice', true);
SELECT * FROM users;
```


## Running tests

```bash
go test ./...
```

## Project structure

```
cmd/
  godb-server/      # REPL entrypoint that wires the engine and storage
internal/
  engine/           # DB engine, execution planner, and simple evaluator
  sql/              # SQL parser and AST definitions
  storage/
    memstore/       # In-memory storage implementation
```

## Architecture

```mermaid
graph TD
  REPL[REPL (cmd/godb-server)] --> Parser[SQL parser]
  Parser --> Engine[Execution engine]
  Engine --> Storage[Storage interface]
  Storage --> Memstore[In-memory store]
```

- `cmd/godb-server` reads input, handles meta commands, and forwards SQL to the engine.
- `internal/sql` parses SQL into AST nodes and validates column types.
- `internal/engine` executes statements (create, insert, select, update, delete) against the storage implementation.
- `internal/storage/memstore` provides an in-memory table storage layer used by default.

## Roadmap (very rough)

- On-disk storage
- Better query planner / optimizer
- Indexes
- Transactions at SQL level (BEGIN / COMMIT / ROLLBACK)
- Maybe: distributed experiments later
