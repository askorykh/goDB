package main

import (
	"bufio"
	"errors"
	"fmt"
	"goDB/internal/storage/filestore"
	"io"
	"log"

	"goDB/internal/engine"
	"goDB/internal/sql"
	"os"
	"strings"
)

func main() {
	fmt.Println("GoDB server starting (REPL mode)…")

	/// choose storage implementation
	// mem := memstore.New()
	// eng := engine.New(mem)

	fs, err := filestore.New("./data")
	if err != nil {
		log.Fatalf("failed to init filestore: %v", err)
	}
	eng := engine.New(fs)

	if err := eng.Start(); err != nil {
		log.Fatalf("engine start failed: %v", err)
	}

	fmt.Println("Engine started successfully (using in-memory storage).")
	fmt.Println("Type SQL statements like:")
	fmt.Println("  CREATE TABLE users (id INT, name STRING, active BOOL);")
	fmt.Println("  INSERT INTO users VALUES (1, 'Alice', true);")
	fmt.Println("  SELECT * FROM users;")
	fmt.Println("Meta commands:")
	fmt.Println("  .tables        - list tables")
	fmt.Println("  .schema <tbl>  - show column definitions")
	fmt.Println("  .exit          - quit")
	fmt.Println("  .help          - show this help")
	fmt.Println()

	runREPL(eng)
}

func runREPL(eng *engine.DBEngine) {
	reader := bufio.NewReader(os.Stdin)
	var buffer strings.Builder

	for {
		prompt := "godb> "
		if buffer.Len() > 0 {
			prompt = "...> "
		}

		fmt.Print(prompt)
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("\nExiting.")
				return
			}

			fmt.Println("Read error:", err)
			return
		}

		line = strings.TrimSpace(line)

		if buffer.Len() == 0 && line == "" {
			continue
		}

		// Meta commands start with a dot, like SQLite. Only process them
		// when no SQL is buffered to avoid mixing with multi-line input.
		if buffer.Len() == 0 && strings.HasPrefix(line, ".") {
			if handleMetaCommand(line, eng) {
				return
			}
			continue
		}

		if line != "" {
			if buffer.Len() > 0 {
				buffer.WriteString(" ")
			}
			buffer.WriteString(line)
		}

		if strings.HasSuffix(line, ";") {
			statement := buffer.String()
			buffer.Reset()
			handleSQL(statement, eng)
		}
	}
}

// handleMetaCommand processes commands like .exit, .help.
// Returns true if the REPL should exit.
func handleMetaCommand(line string, eng *engine.DBEngine) bool {
	trimmed := strings.TrimSpace(line)
	parts := strings.Fields(trimmed)
	if len(parts) == 0 {
		return false
	}

	switch strings.ToLower(parts[0]) {
	case ".exit", ".quit":
		fmt.Println("Bye.")
		return true
	case ".help":
		fmt.Println("Supported SQL (current version):")
		fmt.Println()
		fmt.Println("  CREATE TABLE tableName (")
		fmt.Println("      columnName TYPE, ...")
		fmt.Println("  );")
		fmt.Println("    - Supported types: INT, FLOAT, STRING, BOOL")
		fmt.Println()
		fmt.Println("  INSERT INTO tableName VALUES (value1, value2, ...);")
		fmt.Println("    - Values must match table column order")
		fmt.Println()
		fmt.Println("  SELECT * FROM tableName;")
		fmt.Println("  SELECT col1, col2, ... FROM tableName;")
		fmt.Println("  SELECT col1, col2 FROM tableName WHERE column = literal;")
		fmt.Println("    - WHERE: supports only equality (=)")
		fmt.Println("    - WHERE literals: INT, FLOAT, STRING ('text'), BOOL")
		fmt.Println()
		fmt.Println("Meta commands:")
		fmt.Println("  .tables        List available tables")
		fmt.Println("  .schema <tbl>  Show column definitions")
		fmt.Println("  .help          Show this help")
		fmt.Println("  .exit          Exit the REPL")
		fmt.Println()
		return false
	case ".tables":
		names, err := eng.ListTables()
		if err != nil {
			fmt.Println("Error listing tables:", err)
			return false
		}

		if len(names) == 0 {
			fmt.Println("(no tables)")
			return false
		}

		fmt.Println(strings.Join(names, "\n"))
		return false
	case ".schema":
		if len(parts) < 2 {
			fmt.Println("Usage: .schema <table>")
			return false
		}

		cols, err := eng.TableSchema(parts[1])
		if err != nil {
			fmt.Println("Error loading schema:", err)
			return false
		}

		if len(cols) == 0 {
			fmt.Println("(no columns)")
			return false
		}

		for _, col := range cols {
			fmt.Printf("%s %s\n", col.Name, formatType(col.Type))
		}
		return false

	default:
		fmt.Printf("Unknown meta command: %s\n", trimmed)
	}
	return false
}

func handleSQL(line string, eng *engine.DBEngine) {
	// Allow multi-line-ish usage by adding missing semicolon mentally, but for now
	// we just pass the line as is; parser already handles optional trailing ';'.
	stmt, err := sql.Parse(line)
	if err != nil {
		fmt.Println("Parse error:", err)
		return
	}

	cols, rows, err := eng.Execute(stmt)
	if err != nil {
		fmt.Println("Execution error:", err)
		return
	}

	// If we got columns back, assume it's a SELECT and print a table.
	if len(cols) > 0 {
		printResultSet(cols, rows)
	} else {
		// For CREATE/INSERT we just say OK for now.
		fmt.Println("OK")
	}
}

func printResultSet(cols []string, rows []sql.Row) {
	// Header
	fmt.Println(strings.Join(cols, " | "))

	// Rows
	for _, row := range rows {
		var parts []string
		for _, v := range row {
			parts = append(parts, formatValue(v))
		}
		fmt.Println(strings.Join(parts, " | "))
	}
}

// formatValue converts a sql.Value to a human-readable string.
func formatValue(v sql.Value) string {
	switch v.Type {
	case sql.TypeInt:
		return fmt.Sprintf("%d", v.I64)
	case sql.TypeFloat:
		return fmt.Sprintf("%f", v.F64)
	case sql.TypeString:
		return v.S
	case sql.TypeBool:
		if v.B {
			return "true"
		}
		return "false"
	case sql.TypeNull:
		return "NULL"
	default:
		return "NULL"
	}
}

func formatType(t sql.DataType) string {
	switch t {
	case sql.TypeInt:
		return "INT"
	case sql.TypeFloat:
		return "FLOAT"
	case sql.TypeString:
		return "STRING"
	case sql.TypeBool:
		return "BOOL"
	default:
		return "UNKNOWN"
	}
}
