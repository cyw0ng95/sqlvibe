package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func main() {
	checkFlag := flag.Bool("check", false, "Validate database integrity")
	infoFlag  := flag.Bool("info", false, "Show database metadata")
	tablesFlag := flag.Bool("tables", false, "List tables")
	schemaFlag := flag.String("schema", "", "Show schema for table (empty = all)")
	indexesFlag := flag.Bool("indexes", false, "Show indexes")
	pagesFlag := flag.Bool("pages", false, "Show page statistics")
	verboseFlag := flag.Bool("verbose", false, "Verbose output")

	flag.Parse()

	if flag.NArg() == 0 {
		fmt.Fprintf(os.Stderr, "Usage: sv-check [flags] <database>\n")
		flag.PrintDefaults()
		os.Exit(1)
	}
	dbPath := flag.Arg(0)

	db, err := sqlvibe.Open(dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	exitCode := 0

	if *checkFlag {
		report, err := db.CheckIntegrity()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Integrity check error: %v\n", err)
			os.Exit(1)
		}
		if report.Valid {
			fmt.Println("ok")
		} else {
			fmt.Println("FAILED")
			for _, e := range report.Errors {
				fmt.Fprintf(os.Stderr, "  error: %s\n", e)
			}
			exitCode = 1
		}
		if *verboseFlag {
			fmt.Printf("pages: %d, free: %d\n", report.PageCount, report.FreePages)
		}
	}

	if *infoFlag {
		info, err := db.GetDatabaseInfo()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("file:      %s\n", info.FilePath)
		fmt.Printf("size:      %d bytes\n", info.FileSize)
		fmt.Printf("page_size: %d\n", info.PageSize)
		fmt.Printf("pages:     %d\n", info.PageCount)
		fmt.Printf("wal_mode:  %v\n", info.WALMode)
		fmt.Printf("encoding:  %s\n", info.Encoding)
	}

	if *tablesFlag {
		tables, err := db.GetTables()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		for _, t := range tables {
			if *verboseFlag {
				fmt.Printf("%s (%s)\n", t.Name, t.Type)
			} else {
				fmt.Println(t.Name)
			}
		}
	}

	if flag.Lookup("schema").Value.String() != "" || *schemaFlag != "" {
		tableName := *schemaFlag
		if tableName == "" {
			// show all
			tables, err := db.GetTables()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			for _, t := range tables {
				fmt.Println(t.SQL + ";")
			}
		} else {
			sql, err := db.GetSchema(tableName)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error: %v\n", err)
				os.Exit(1)
			}
			fmt.Println(sql + ";")
		}
	}

	if *indexesFlag {
		indexes, err := db.GetIndexes("")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
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

	if *pagesFlag {
		stats, err := db.GetPageStats()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("leaf_pages:     %d\n", stats.LeafPages)
		fmt.Printf("interior_pages: %d\n", stats.InteriorPages)
		fmt.Printf("overflow_pages: %d\n", stats.OverflowPages)
		fmt.Printf("total_pages:    %d\n", stats.TotalPages)
	}

	os.Exit(exitCode)
}
