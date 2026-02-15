package main

import (
	"database/sql"
	"fmt"
	_ "github.com/glebarez/go-sqlite"
)

func fetchAllRowsSQLite(rows *sql.Rows) ([]map[string]interface{}, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]interface{}
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}
		results = append(results, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}

func main() {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		fmt.Println("Open error:", err)
		return
	}
	defer db.Close()
	
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	db.Exec("INSERT INTO test VALUES (1, 'one'), (2, NULL), (3, 'three')")
	
	// Test COALESCE
	rows1, _ := db.Query("SELECT id, COALESCE(value, 'default') FROM test")
	data1, _ := fetchAllRowsSQLite(rows1)
	fmt.Println("COALESCE results:")
	for i, row := range data1 {
		fmt.Printf("  row %d: %v\n", i, row)
	}
	
	// Test IFNULL
	rows2, _ := db.Query("SELECT id, IFNULL(value, 'default') FROM test")
	data2, _ := fetchAllRowsSQLite(rows2)
	fmt.Println("\nIFNULL results:")
	for i, row := range data2 {
		fmt.Printf("  row %d: %v\n", i, row)
	}
}
