package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
)

func main() {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		fmt.Println("Open error:", err)
		return
	}
	defer db.Close()
	
	_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		fmt.Println("Create table error:", err)
		return
	}
	_, err = db.Exec("INSERT INTO test VALUES (1, 'one'), (2, NULL), (3, 'three')")
	if err != nil {
		fmt.Println("Insert error:", err)
		return
	}
	
	rows, err := db.Query("SELECT id, IFNULL(value, 'default') FROM test")
	if err != nil {
		fmt.Println("Query IFNULL error:", err)
		return
	}
	defer rows.Close()
	
	columns, _ := rows.Columns()
	fmt.Println("Columns:", columns)
	
	for rows.Next() {
		var id int
		var result interface{}
		rows.Scan(&id, &result)
		fmt.Printf("id=%d, IFNULL result=%v (type=%T)\n", id, result, result)
	}
	
	rows2, err := db.Query("SELECT id, COALESCE(value, 'default') FROM test")
	if err != nil {
		fmt.Println("Query COALESCE error:", err)
		return
	}
	defer rows2.Close()
	
	for rows2.Next() {
		var id int
		var result interface{}
		rows2.Scan(&id, &result)
		fmt.Printf("id=%d, COALESCE result=%v (type=%T)\n", id, result, result)
	}
}
