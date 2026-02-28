package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
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

	fmt.Println("sv-cli - sqlvibe database shell")
	fmt.Println("Type '.help' for help, 'exit' or 'quit' to exit")
	fmt.Println()

	// Create formatter
	formatter := NewFormatter()
	importer := NewImporter(db)
	exporter := NewExporter(db)

	// State
	timerEnabled := false
	nullValue := ""
	outputFile := ""

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
			if handleMetaCommand(db, line, formatter, importer, exporter, &timerEnabled, &nullValue, &outputFile) {
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

		// Time the query
		startTime := time.Now()

		rows, err := db.Query(line)
		execTime := time.Since(startTime)

		if err != nil {
			result, execErr := db.Exec(line)
			if execErr != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				continue
			}
			if result.RowsAffected > 0 {
				fmt.Printf("%d row(s) affected\n", result.RowsAffected)
			}
			if timerEnabled {
				fmt.Printf("Run Time: real %.3f ms\n", float64(execTime.Microseconds())/1000.0)
			}
			continue
		}

		if rows != nil && len(rows.Columns) > 0 {
			// Format output
			output := formatter.Format(rows)
			
			// Redirect to file if set
			if outputFile != "" {
				f, err := os.OpenFile(outputFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error opening output file: %v\n", err)
				} else {
					f.WriteString(output)
					f.Close()
				}
			} else {
				fmt.Print(output)
			}

			if timerEnabled {
				fmt.Printf("Run Time: real %.3f ms\n", float64(execTime.Microseconds())/1000.0)
			}
		}
	}
}

// handleMetaCommand processes dot commands. Returns true to exit the shell.
func handleMetaCommand(db *sqlvibe.Database, line string, formatter *Formatter, importer *Importer, exporter *Exporter, timerEnabled *bool, nullValue *string, outputFile *string) bool {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return false
	}
	
	cmd := strings.ToLower(parts[0])
	args := parts[1:]

	switch cmd {
	case ".exit", ".quit":
		return true

	case ".help":
		printHelp()

	case ".tables":
		listTables(db)

	case ".schema":
		showSchema(db, args)

	case ".indexes":
		showIndexes(db, args)

	case ".ext":
		showExtensions(db)

	case ".mode":
		setOutputMode(formatter, args)

	case ".headers":
		toggleHeaders(formatter, args)

	case ".timer":
		toggleTimer(timerEnabled, args)

	case ".nullvalue":
		setNullValue(formatter, args)

	case ".width":
		setColumnWidths(formatter, args)

	case ".import":
		importCSV(importer, args)

	case ".export":
		exportData(exporter, args)

	case ".read":
		readSQLFile(db, args)

	case ".output":
		setOutput(outputFile, args)

	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s (try .help)\n", parts[0])
	}
	return false
}

func printHelp() {
	fmt.Println("Database:")
	fmt.Println("  .tables [TABLE]       List all tables or show tables matching pattern")
	fmt.Println("  .schema [TABLE]       Show CREATE statement(s)")
	fmt.Println("  .indexes [TABLE]      Show indexes")
	fmt.Println("  .ext                  List loaded extensions")
	fmt.Println()
	fmt.Println("Output:")
	fmt.Println("  .mode MODE            Set output mode (csv, table, list, json)")
	fmt.Println("  .headers on|off       Toggle column headers")
	fmt.Println("  .nullvalue TEXT       Set string for NULL values")
	fmt.Println("  .width N1 N2 ...      Set column widths")
	fmt.Println("  .output FILE          Redirect output to file")
	fmt.Println("  .output stdout        Restore output to stdout")
	fmt.Println()
	fmt.Println("I/O:")
	fmt.Println("  .import FILE TABLE    Import CSV file into table")
	fmt.Println("  .export csv FILE      Export current table to CSV")
	fmt.Println("  .export json FILE     Export current table to JSON")
	fmt.Println("  .read FILE            Execute SQL from file")
	fmt.Println()
	fmt.Println("Other:")
	fmt.Println("  .timer on|off         Toggle query timer")
	fmt.Println("  .help                 Show this help")
	fmt.Println("  .exit, .quit          Exit the shell")
}

func listTables(db *sqlvibe.Database) {
	tables, err := db.GetTables()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}
	for _, t := range tables {
		fmt.Println(t.Name)
	}
}

func showSchema(db *sqlvibe.Database, args []string) {
	if len(args) > 0 {
		tableName := args[0]
		sql, err := db.GetSchema(tableName)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return
		}
		fmt.Println(sql + ";")
	} else {
		tables, err := db.GetTables()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			return
		}
		for _, t := range tables {
			fmt.Println(t.SQL + ";")
		}
	}
}

func showIndexes(db *sqlvibe.Database, args []string) {
	tableName := ""
	if len(args) > 0 {
		tableName = args[0]
	}
	indexes, err := db.GetIndexes(tableName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}
	for _, idx := range indexes {
		unique := ""
		if idx.Unique {
			unique = "UNIQUE "
		}
		fmt.Printf("CREATE %sINDEX %s ON %s (%s);\n",
			unique, idx.Name, idx.Table, strings.Join(idx.Columns, ", "))
	}
}

func showExtensions(db *sqlvibe.Database) {
	rows, err := db.Query("SELECT * FROM sqlvibe_extensions")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}
	if rows == nil || len(rows.Columns) == 0 {
		fmt.Println("No extensions loaded.")
		return
	}
	formatter := NewFormatter()
	formatter.SetShowHeaders(true)
	fmt.Print(formatter.Format(rows))
}

func setOutputMode(formatter *Formatter, args []string) {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: .mode MODE (csv, table, list, json)\n")
		return
	}
	switch strings.ToLower(args[0]) {
	case "csv":
		formatter.SetMode(OutputCSV)
	case "table":
		formatter.SetMode(OutputTable)
	case "list":
		formatter.SetMode(OutputList)
	case "json":
		formatter.SetMode(OutputJSON)
	default:
		fmt.Fprintf(os.Stderr, "Unknown mode: %s\n", args[0])
	}
}

func toggleHeaders(formatter *Formatter, args []string) {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: .headers on|off\n")
		return
	}
	switch strings.ToLower(args[0]) {
	case "on":
		formatter.SetShowHeaders(true)
	case "off":
		formatter.SetShowHeaders(false)
	default:
		fmt.Fprintf(os.Stderr, "Usage: .headers on|off\n")
	}
}

func toggleTimer(enabled *bool, args []string) {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: .timer on|off\n")
		return
	}
	switch strings.ToLower(args[0]) {
	case "on":
		*enabled = true
	case "off":
		*enabled = false
	default:
		fmt.Fprintf(os.Stderr, "Usage: .timer on|off\n")
	}
}

func setNullValue(formatter *Formatter, args []string) {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: .nullvalue TEXT\n")
		return
	}
	formatter.SetNullValue(args[0])
}

func setColumnWidths(formatter *Formatter, args []string) {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: .width N1 N2 ...\n")
		return
	}
	widths := make([]int, len(args))
	for i, arg := range args {
		w, err := strconv.Atoi(arg)
		if err != nil {
			widths[i] = -1 // auto
		} else {
			widths[i] = w
		}
	}
	formatter.SetColumnWidths(widths)
}

func importCSV(importer *Importer, args []string) {
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: .import FILE TABLE\n")
		return
	}
	count, err := importer.ImportCSV(args[0], args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		return
	}
	fmt.Printf("Imported %d rows\n", count)
}

func exportData(exporter *Exporter, args []string) {
	if len(args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: .export csv|json FILE\n")
		return
	}
	format := strings.ToLower(args[0])
	filename := args[1]
	
	// For now, export current table (would need to track current table)
	fmt.Fprintf(os.Stderr, "Export format '%s' to '%s' - requires table name\n", format, filename)
}

func readSQLFile(db *sqlvibe.Database, args []string) {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: .read FILE\n")
		return
	}
	err := ExecuteSQLFile(db, args[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	}
}

func setOutput(outputFile *string, args []string) {
	if len(args) == 0 {
		fmt.Fprintf(os.Stderr, "Usage: .output FILE\n")
		return
	}
	*outputFile = args[0]
	if args[0] == "stdout" {
		*outputFile = ""
		fmt.Println("Output redirected to stdout")
	} else {
		fmt.Printf("Output redirected to '%s'\n", args[0])
	}
}
