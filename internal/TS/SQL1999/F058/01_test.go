package F058

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F058_ScalarSubquery_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, name TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, score INTEGER)"},
		{"InsertT1_1", "INSERT INTO t1 VALUES (1, 'Alice')"},
		{"InsertT1_2", "INSERT INTO t1 VALUES (2, 'Bob')"},
		{"InsertT1_3", "INSERT INTO t1 VALUES (3, 'Carol')"},
		{"InsertT2_1", "INSERT INTO t2 VALUES (1, 90)"},
		{"InsertT2_2", "INSERT INTO t2 VALUES (1, 85)"},
		{"InsertT2_3", "INSERT INTO t2 VALUES (2, 70)"},
		{"InsertT2_4", "INSERT INTO t2 VALUES (3, 95)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"ScalarSubqueryInWhere", "SELECT name FROM t1 WHERE id = (SELECT id FROM t2 ORDER BY score DESC LIMIT 1)"},
		{"ScalarSubqueryMax", "SELECT (SELECT MAX(score) FROM t2) AS global_max"},
		{"ScalarSubqueryMin", "SELECT (SELECT MIN(score) FROM t2) AS global_min"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F058_ExistsSubquery_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateUsers", "CREATE TABLE users (id INTEGER, name TEXT)"},
		{"CreateOrders", "CREATE TABLE orders (id INTEGER, user_id INTEGER, amount INTEGER)"},
		{"InsertUser1", "INSERT INTO users VALUES (1, 'Alice')"},
		{"InsertUser2", "INSERT INTO users VALUES (2, 'Bob')"},
		{"InsertUser3", "INSERT INTO users VALUES (3, 'Carol')"},
		{"InsertOrder1", "INSERT INTO orders VALUES (1, 1, 100)"},
		{"InsertOrder2", "INSERT INTO orders VALUES (2, 1, 200)"},
		{"InsertOrder3", "INSERT INTO orders VALUES (3, 3, 50)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"ExistsUsers", "SELECT name FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY name"},
		{"NotExistsUsers", "SELECT name FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id) ORDER BY name"},
		{"ExistsAtLeastOneOrder", "SELECT COUNT(*) FROM orders"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F058_InSubquery_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateProducts", "CREATE TABLE products (id INTEGER, name TEXT, cat_id INTEGER)"},
		{"CreateCategories", "CREATE TABLE categories (id INTEGER, label TEXT)"},
		{"InsertCat1", "INSERT INTO categories VALUES (1, 'Electronics')"},
		{"InsertCat2", "INSERT INTO categories VALUES (2, 'Books')"},
		{"InsertCat3", "INSERT INTO categories VALUES (3, 'Toys')"},
		{"InsertProd1", "INSERT INTO products VALUES (1, 'Phone', 1)"},
		{"InsertProd2", "INSERT INTO products VALUES (2, 'Novel', 2)"},
		{"InsertProd3", "INSERT INTO products VALUES (3, 'Laptop', 1)"},
		{"InsertProd4", "INSERT INTO products VALUES (4, 'Puzzle', 3)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"InSubquery", "SELECT name FROM products WHERE cat_id IN (SELECT id FROM categories WHERE label = 'Electronics') ORDER BY name"},
		{"NotInSubquery", "SELECT name FROM products WHERE cat_id NOT IN (SELECT id FROM categories WHERE label = 'Toys') ORDER BY name"},
		{"InSubqueryAllCats", "SELECT name FROM products WHERE cat_id IN (SELECT id FROM categories) ORDER BY name"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F058_NonCorrelatedSubquery_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateEmployees", "CREATE TABLE employees (id INTEGER, dept_id INTEGER, salary INTEGER, name TEXT)"},
		{"InsertEmp1", "INSERT INTO employees VALUES (1, 1, 5000, 'Alice')"},
		{"InsertEmp2", "INSERT INTO employees VALUES (2, 1, 6000, 'Bob')"},
		{"InsertEmp3", "INSERT INTO employees VALUES (3, 2, 4500, 'Carol')"},
		{"InsertEmp4", "INSERT INTO employees VALUES (4, 2, 7000, 'Dave')"},
		{"InsertEmp5", "INSERT INTO employees VALUES (5, 1, 5500, 'Eve')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"AboveGlobalAvg", "SELECT name, salary FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) ORDER BY name"},
		{"MaxSalaryEmployee", "SELECT name FROM employees WHERE salary = (SELECT MAX(salary) FROM employees)"},
		{"MinSalaryEmployee", "SELECT name FROM employees WHERE salary = (SELECT MIN(salary) FROM employees)"},
		{"InDept1", "SELECT name FROM employees WHERE dept_id IN (SELECT DISTINCT dept_id FROM employees WHERE salary > 5000) ORDER BY name"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F058_SubqueryInFrom_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE numbers (id INTEGER, val INTEGER)"},
		{"Insert1", "INSERT INTO numbers VALUES (1, 3)"},
		{"Insert2", "INSERT INTO numbers VALUES (2, 7)"},
		{"Insert3", "INSERT INTO numbers VALUES (3, 2)"},
		{"Insert4", "INSERT INTO numbers VALUES (4, 9)"},
		{"Insert5", "INSERT INTO numbers VALUES (5, 5)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SubqueryFromClause", "SELECT * FROM (SELECT id, val FROM numbers WHERE val > 5) AS subq ORDER BY id"},
		{"SubqueryFromCount", "SELECT COUNT(*) FROM (SELECT id FROM numbers WHERE val > 3) AS big"},
		{"SubqueryFromSum", "SELECT SUM(val) FROM (SELECT val FROM numbers WHERE val > 4) AS filtered"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
