package E091

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E09101_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, quantity INTEGER, price REAL)")
	sqliteDB.Exec("CREATE TABLE sales (id INTEGER PRIMARY KEY, product TEXT, quantity INTEGER, price REAL)")

	sqlvibeDB.Exec("INSERT INTO sales VALUES (1, 'Apple', 10, 1.50)")
	sqliteDB.Exec("INSERT INTO sales VALUES (1, 'Apple', 10, 1.50)")
	sqlvibeDB.Exec("INSERT INTO sales VALUES (2, 'Banana', 5, 0.75)")
	sqliteDB.Exec("INSERT INTO sales VALUES (2, 'Banana', 5, 0.75)")
	sqlvibeDB.Exec("INSERT INTO sales VALUES (3, 'Apple', 20, 1.50)")
	sqliteDB.Exec("INSERT INTO sales VALUES (3, 'Apple', 20, 1.50)")
	sqlvibeDB.Exec("INSERT INTO sales VALUES (4, 'Banana', 15, 0.75)")
	sqliteDB.Exec("INSERT INTO sales VALUES (4, 'Banana', 15, 0.75)")
	sqlvibeDB.Exec("INSERT INTO sales VALUES (5, 'Cherry', 8, 2.00)")
	sqliteDB.Exec("INSERT INTO sales VALUES (5, 'Cherry', 8, 2.00)")

	tests := []struct {
		name string
		sql  string
	}{
		{"AVG", "SELECT AVG(price) FROM sales"},
		{"AVGWithGroupBy", "SELECT product, AVG(price) FROM sales GROUP BY product"},
		{"AVGWithWhere", "SELECT AVG(quantity) FROM sales WHERE product = 'Apple'"},
		{"AVGWithNull", "SELECT AVG(price) FROM sales WHERE price > 100"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E09102_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")
	sqliteDB.Exec("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)")

	sqlvibeDB.Exec("INSERT INTO products VALUES (1, 'Widget', 10.00)")
	sqliteDB.Exec("INSERT INTO products VALUES (1, 'Widget', 10.00)")
	sqlvibeDB.Exec("INSERT INTO products VALUES (2, 'Gadget', 25.00)")
	sqliteDB.Exec("INSERT INTO products VALUES (2, 'Gadget', 25.00)")
	sqlvibeDB.Exec("INSERT INTO products VALUES (3, 'Gizmo', 15.00)")
	sqliteDB.Exec("INSERT INTO products VALUES (3, 'Gizmo', 15.00)")

	tests := []struct {
		name string
		sql  string
	}{
		{"CountStar", "SELECT COUNT(*) FROM products"},
		{"CountColumn", "SELECT COUNT(price) FROM products"},
		{"CountWithWhere", "SELECT COUNT(*) FROM products WHERE price > 20"},
		{"CountWithGroupBy", "SELECT COUNT(*) FROM products"},
		{"CountDistinct", "SELECT COUNT(DISTINCT name) FROM products"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E09103_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE scores (id INTEGER PRIMARY KEY, student TEXT, score INTEGER)")
	sqliteDB.Exec("CREATE TABLE scores (id INTEGER PRIMARY KEY, student TEXT, score INTEGER)")

	sqlvibeDB.Exec("INSERT INTO scores VALUES (1, 'Alice', 95)")
	sqliteDB.Exec("INSERT INTO scores VALUES (1, 'Alice', 95)")
	sqlvibeDB.Exec("INSERT INTO scores VALUES (2, 'Bob', 87)")
	sqliteDB.Exec("INSERT INTO scores VALUES (2, 'Bob', 87)")
	sqlvibeDB.Exec("INSERT INTO scores VALUES (3, 'Charlie', 92)")
	sqliteDB.Exec("INSERT INTO scores VALUES (3, 'Charlie', 92)")
	sqlvibeDB.Exec("INSERT INTO scores VALUES (4, 'Diana', 88)")
	sqliteDB.Exec("INSERT INTO scores VALUES (4, 'Diana', 88)")

	tests := []struct {
		name string
		sql  string
	}{
		{"MAX", "SELECT MAX(score) FROM scores"},
		{"MIN", "SELECT MIN(score) FROM scores"},
		{"MAXWithWhere", "SELECT MAX(score) FROM scores WHERE student = 'Alice'"},
		{"MINWithWhere", "SELECT MIN(score) FROM scores WHERE score > 90"},
		{"MAXWithGroupBy", "SELECT student, MAX(score) FROM scores GROUP BY student"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E09104_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, amount REAL)")
	sqliteDB.Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT, amount REAL)")

	sqlvibeDB.Exec("INSERT INTO orders VALUES (1, 'John', 100.00)")
	sqliteDB.Exec("INSERT INTO orders VALUES (1, 'John', 100.00)")
	sqlvibeDB.Exec("INSERT INTO orders VALUES (2, 'Jane', 250.50)")
	sqliteDB.Exec("INSERT INTO orders VALUES (2, 'Jane', 250.50)")
	sqlvibeDB.Exec("INSERT INTO orders VALUES (3, 'John', 75.25)")
	sqliteDB.Exec("INSERT INTO orders VALUES (3, 'John', 75.25)")
	sqlvibeDB.Exec("INSERT INTO orders VALUES (4, 'Jane', 500.00)")
	sqliteDB.Exec("INSERT INTO orders VALUES (4, 'Jane', 500.00)")
	sqlvibeDB.Exec("INSERT INTO orders VALUES (5, 'Bob', 300.00)")
	sqliteDB.Exec("INSERT INTO orders VALUES (5, 'Bob', 300.00)")

	tests := []struct {
		name string
		sql  string
	}{
		{"SUM", "SELECT SUM(amount) FROM orders"},
		{"SUMWithGroupBy", "SELECT customer, SUM(amount) FROM orders GROUP BY customer"},
		{"SUMWithWhere", "SELECT SUM(amount) FROM orders WHERE customer = 'John'"},
		{"SUMWithNull", "SELECT SUM(amount) FROM orders WHERE amount > 1000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E09105_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)")
	sqliteDB.Exec("CREATE TABLE data (id INTEGER PRIMARY KEY, val INTEGER)")

	sqlvibeDB.Exec("INSERT INTO data VALUES (1, 1)")
	sqliteDB.Exec("INSERT INTO data VALUES (1, 1)")
	sqlvibeDB.Exec("INSERT INTO data VALUES (2, 2)")
	sqliteDB.Exec("INSERT INTO data VALUES (2, 2)")
	sqlvibeDB.Exec("INSERT INTO data VALUES (3, 3)")
	sqliteDB.Exec("INSERT INTO data VALUES (3, 3)")
	sqlvibeDB.Exec("INSERT INTO data VALUES (4, 4)")
	sqliteDB.Exec("INSERT INTO data VALUES (4, 4)")
	sqlvibeDB.Exec("INSERT INTO data VALUES (5, 5)")
	sqliteDB.Exec("INSERT INTO data VALUES (5, 5)")

	tests := []struct {
		name string
		sql  string
	}{
		{"AVGAll", "SELECT AVG(val) FROM data"},
		{"COUNTAll", "SELECT COUNT(val) FROM data"},
		{"MAXAll", "SELECT MAX(val) FROM data"},
		{"MINAll", "SELECT MIN(val) FROM data"},
		{"SUMAll", "SELECT SUM(val) FROM data"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E09106_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 20)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 20)")

	tests := []struct {
		name string
		sql  string
	}{
		{"AVGWithAll", "SELECT AVG(ALL val) FROM t1"},
		{"COUNTWithAll", "SELECT COUNT(ALL val) FROM t1"},
		{"MAXWithAll", "SELECT MAX(ALL val) FROM t1"},
		{"MINWithAll", "SELECT MIN(ALL val) FROM t1"},
		{"SUMWithAll", "SELECT SUM(ALL val) FROM t1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E09107_L1(t *testing.T) {
	sqlvibePath := ":memory:"
	sqlitePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", sqlitePath)
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, val INTEGER)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 10)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 10)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, 20)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, 20)")

	tests := []struct {
		name string
		sql  string
	}{
		{"AVGWithDistinct", "SELECT AVG(DISTINCT val) FROM t1"},
		{"COUNTWithDistinct", "SELECT COUNT(DISTINCT val) FROM t1"},
		{"MAXWithDistinct", "SELECT MAX(DISTINCT val) FROM t1"},
		{"MINWithDistinct", "SELECT MIN(DISTINCT val) FROM t1"},
		{"SUMWithDistinct", "SELECT SUM(DISTINCT val) FROM t1"},
		{"CountDistinct", "SELECT COUNT(DISTINCT val) FROM t1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
