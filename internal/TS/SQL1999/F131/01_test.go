package F131

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F131_GroupBySimple_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE employees (dept TEXT, salary INTEGER)"},
		{"InsertData", "INSERT INTO employees VALUES ('IT', 5000), ('IT', 6000), ('HR', 4500), ('HR', 5500), ('Sales', 4000)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"GroupByDept", "SELECT dept, COUNT(*) FROM employees GROUP BY dept"},
		{"GroupByDeptSum", "SELECT dept, SUM(salary) FROM employees GROUP BY dept"},
		{"GroupByDeptAvg", "SELECT dept, AVG(salary) FROM employees GROUP BY dept"},
		{"GroupByDeptMin", "SELECT dept, MIN(salary) FROM employees GROUP BY dept"},
		{"GroupByDeptMax", "SELECT dept, MAX(salary) FROM employees GROUP BY dept"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F131_GroupByMultiple_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE sales (region TEXT, product TEXT, amount INTEGER)"},
		{"InsertData", "INSERT INTO sales VALUES ('North', 'A', 100), ('North', 'B', 200), ('South', 'A', 150), ('South', 'B', 250)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"GroupByTwo", "SELECT region, product, SUM(amount) FROM sales GROUP BY region, product"},
		{"GroupByWithOrder", "SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F131_Having_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE orders (customer TEXT, total INTEGER)"},
		{"InsertData", "INSERT INTO orders VALUES ('A', 100), ('A', 200), ('B', 50), ('B', 150), ('C', 300)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"HavingSum", "SELECT customer, SUM(total) FROM orders GROUP BY customer HAVING SUM(total) > 200"},
		{"HavingCount", "SELECT customer, COUNT(*) FROM orders GROUP BY customer HAVING COUNT(*) > 1"},
		{"HavingAvg", "SELECT customer, AVG(total) FROM orders GROUP BY customer HAVING AVG(total) >= 100"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F131_Distinct_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE items (category TEXT, value INTEGER)"},
		{"InsertData", "INSERT INTO items VALUES ('A', 1), ('A', 2), ('B', 1), ('B', 1), ('C', 3)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"DistinctSingle", "SELECT DISTINCT category FROM items"},
		{"DistinctCount", "SELECT COUNT(DISTINCT category) FROM items"},
		{"DistinctWithAgg", "SELECT category, COUNT(*) FROM items GROUP BY category"},
		{"DistinctWithOrder", "SELECT DISTINCT category FROM items ORDER BY category"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
