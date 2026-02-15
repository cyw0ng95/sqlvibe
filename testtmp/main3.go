package main

import (
	"database/sql"
	"fmt"
	_ "github.com/glebarez/go-sqlite"
)

func main() {
	db, _ := sql.Open("sqlite", ":memory:")
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	db.Exec("INSERT INTO test VALUES (1, 'one'), (2, NULL), (3, 'three')")
	
	// Test COALESCE
	rows1, _ := db.Query("SELECT id, COALESCE(value, 'default') FROM test")
	cols1, _ := rows1.Columns()
	fmt.Println("SQLite COALESCE columns:", cols1)
	rows1.Close()
	
	// Test IFNULL  
	rows2, _ := db.Query("SELECT id, IFNULL(value, 'default') FROM test")
	cols2, _ := rows2.Columns()
	fmt.Println("SQLite IFNULL columns:", cols2)
	rows2.Close()
}
