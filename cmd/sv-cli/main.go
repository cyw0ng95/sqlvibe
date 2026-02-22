package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

var (
	dbPath     = flag.String("db", "test.db", "Database file path")
	echoMode   = flag.Bool("echo", false, "Echo SQL statements")
	showHeaders = flag.Bool("headers", true, "Show column headers")
)

func main() {
	flag.Parse()

	db, err := sqlvibe.Open(*dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	args := flag.Args()
	if len(args) > 0 {
		sql := strings.Join(args, " ")
		if *echoMode {
			fmt.Println(sql)
		}
		result, err := db.Exec(sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		if result.RowsAffected > 0 {
			fmt.Printf("%d row(s) affected\n", result.RowsAffected)
		}
		return
	}

	fmt.Println("sv-cli - sqlvibe database shell")
	fmt.Println("Type '.help' for help, 'exit' or 'quit' to exit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("sv-cli> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, ".") {
			if handleMetaCommand(db, line) {
				break
			}
			continue
		}

		if line == "exit" || line == "quit" {
			break
		}

		if *echoMode {
			fmt.Println(line)
		}

		rows, err := db.Query(line)
		if err != nil {
			result, execErr := db.Exec(line)
			if execErr != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			if result.RowsAffected > 0 {
				fmt.Printf("%d row(s) affected\n", result.RowsAffected)
			}
			continue
		}

		if rows != nil && len(rows.Columns) > 0 {
			if *showHeaders {
				fmt.Println(strings.Join(rows.Columns, " | "))
				fmt.Println(strings.Repeat("-", len(strings.Join(rows.Columns, " | "))))
			}
			for _, row := range rows.Data {
				fmt.Println(strings.Join(formatRow(row), " | "))
			}
		}
	}
}

func formatRow(row []interface{}) []string {
	result := make([]string, len(row))
	for i, v := range row {
		if v == nil {
			result[i] = "NULL"
		} else {
			result[i] = fmt.Sprintf("%v", v)
		}
	}
	return result
}

// handleMetaCommand processes dot commands. Returns true to exit the shell.
func handleMetaCommand(db *sqlvibe.Database, line string) bool {
	parts := strings.Fields(line)
	cmd := strings.ToLower(parts[0])

	switch cmd {
	case ".exit", ".quit":
		return true

	case ".help":
		fmt.Println(".tables              List all tables")
		fmt.Println(".schema [table]      Show CREATE statement(s)")
		fmt.Println(".indexes [table]     Show indexes")
		fmt.Println(".headers on|off      Toggle column headers")
		fmt.Println(".exit, .quit         Exit the shell")

	case ".tables":
		tables, err := db.GetTables()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return false
		}
		for _, t := range tables {
			fmt.Println(t.Name)
		}

	case ".schema":
		if len(parts) > 1 {
			tableName := parts[1]
			sql, err := db.GetSchema(tableName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				return false
			}
			fmt.Println(sql + ";")
		} else {
			tables, err := db.GetTables()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				return false
			}
			for _, t := range tables {
				fmt.Println(t.SQL + ";")
			}
		}

	case ".indexes":
		tableName := ""
		if len(parts) > 1 {
			tableName = parts[1]
		}
		indexes, err := db.GetIndexes(tableName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return false
		}
		for _, idx := range indexes {
			unique := ""
			if idx.Unique {
				unique = "UNIQUE "
			}
			fmt.Printf("CREATE %sINDEX %s ON %s (%s);\n",
				unique, idx.Name, idx.Table, strings.Join(idx.Columns, ", "))
		}

	case ".headers":
		if len(parts) > 1 {
			switch strings.ToLower(parts[1]) {
			case "on":
				*showHeaders = true
			case "off":
				*showHeaders = false
			default:
				fmt.Fprintf(os.Stderr, "Usage: .headers on|off\n")
			}
		} else {
			if *showHeaders {
				fmt.Println("headers: on")
			} else {
				fmt.Println("headers: off")
			}
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s (try .help)\n", parts[0])
	}
	return false
}
