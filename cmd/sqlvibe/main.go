package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

var (
	dbPath   = flag.String("db", "test.db", "Database file path")
	echoMode = flag.Bool("echo", false, "Echo SQL statements")
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

	fmt.Println("sqlvibe - SQLite-compatible database engine")
	fmt.Println("Type 'exit' or 'quit' to exit")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("sqlvibe> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		if strings.HasPrefix(line, ".") {
			if handleMetaCommand(line) {
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

		result, err := db.Exec(line)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			continue
		}

		if result.RowsAffected > 0 {
			fmt.Printf("%d row(s) affected\n", result.RowsAffected)
		} else {
			rows, err := db.Query(line)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			if rows != nil && rows.Columns != nil {
				fmt.Println(strings.Join(rows.Columns, " | "))
				for _, row := range rows.Data {
					fmt.Println(strings.Join(formatRow(row), " | "))
				}
			}
		}
	}
}

func formatRow(row []interface{}) []string {
	result := make([]string, len(row))
	for i, v := range row {
		result[i] = fmt.Sprintf("%v", v)
	}
	return result
}

func handleMetaCommand(line string) bool {
	switch strings.ToLower(line) {
	case ".exit", ".quit":
		return true
	case ".tables":
		fmt.Println("(tables not yet implemented)")
	case ".schema":
		fmt.Println("(schema not yet implemented)")
	default:
		fmt.Printf("Unknown command: %s\n", line)
	}
	return false
}
