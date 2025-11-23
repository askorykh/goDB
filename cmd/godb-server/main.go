package main

import (
	"bufio"
	"fmt"

	"goDB/internal/engine"
	"goDB/internal/sql"
	"goDB/internal/storage/memstore"
	"os"
	"strings"
)

func main() {
	fmt.Println("GoDB server starting (REPL mode)…")

	// Create the in-memory storage engine.
	store := memstore.New()

	// Create the DB engine on top of this storage.
	eng := engine.New(store)

	// Start the engine.
	if err := eng.Start(); err != nil {
		fmt.Println("ERROR:", err)
		return
	}

	fmt.Println("Engine started successfully (using in-memory storage).")
	fmt.Println("Type SQL statements like:")
	fmt.Println("  CREATE TABLE users (id INT, name STRING, active BOOL);")
	fmt.Println("  INSERT INTO users VALUES (1, 'Alice', true);")
	fmt.Println("  SELECT * FROM users;")
	fmt.Println("Meta commands:")
	fmt.Println("  .exit   - quit")
	fmt.Println("  .help   - show this help")
	fmt.Println()

	runREPL(eng)
}

func runREPL(eng *engine.DBEngine) {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("godb> ")

		if !scanner.Scan() {
			// EOF (Ctrl+D) or input error
			fmt.Println("\nExiting.")
			return
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Meta commands start with a dot, like SQLite.
		if strings.HasPrefix(line, ".") {
			if handleMetaCommand(line) {
				return
			}
			continue
		}

		handleSQL(line, eng)
	}
}

// handleMetaCommand processes commands like .exit, .help.
// Returns true if the REPL should exit.
func handleMetaCommand(line string) bool {
	switch strings.ToLower(strings.TrimSpace(line)) {
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
		fmt.Println("  .help   Show help")
		fmt.Println("  .exit   Exit the REPL")
		fmt.Println()
		return false

	default:
		fmt.Printf("Unknown meta command: %s\n", line)
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
	default:
		return "NULL"
	}
}
