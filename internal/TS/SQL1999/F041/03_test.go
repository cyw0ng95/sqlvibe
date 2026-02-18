package F041

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F403_F04103_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE employees (id INTEGER, name TEXT, dept TEXT, salary REAL)"},
		{"InsertData1", "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 75000)"},
		{"InsertData2", "INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 80000)"},
		{"InsertData3", "INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 65000)"},
		{"InsertData4", "INSERT INTO employees VALUES (4, 'Diana', 'Sales', 70000)"},
		{"InsertData5", "INSERT INTO employees VALUES (5, 'Eve', 'HR', 60000)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	selectTests := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT * FROM employees"},
		{"SelectColumns", "SELECT name, salary FROM employees"},
		{"SelectWhereEq", "SELECT * FROM employees WHERE dept = 'Engineering'"},
		{"SelectWhereGt", "SELECT * FROM employees WHERE salary > 70000"},
		{"SelectWhereLt", "SELECT * FROM employees WHERE salary < 70000"},
		{"SelectWhereNe", "SELECT * FROM employees WHERE dept <> 'Sales'"},
		{"SelectWhereAnd", "SELECT * FROM employees WHERE dept = 'Engineering' AND salary > 75000"},
		{"SelectWhereOr", "SELECT * FROM employees WHERE dept = 'HR' OR salary > 75000"},
		{"SelectOrderBy", "SELECT * FROM employees ORDER BY salary"},
		{"SelectOrderByDesc", "SELECT * FROM employees ORDER BY salary DESC"},
		{"SelectOrderByName", "SELECT * FROM employees ORDER BY name"},
		{"SelectLimit", "SELECT * FROM employees LIMIT 3"},
		{"SelectLimitOffset", "SELECT * FROM employees LIMIT 2 OFFSET 2"},
		{"SelectDistinct", "SELECT DISTINCT dept FROM employees"},
		{"SelectCount", "SELECT COUNT(*) FROM employees"},
		{"SelectSum", "SELECT SUM(salary) FROM employees"},
		{"SelectAvg", "SELECT AVG(salary) FROM employees"},
		{"SelectMax", "SELECT MAX(salary) FROM employees"},
		{"SelectMin", "SELECT MIN(salary) FROM employees"},
		{"SelectGroupBy", "SELECT dept, COUNT(*) FROM employees GROUP BY dept"},
		{"SelectGroupBySum", "SELECT dept, SUM(salary) FROM employees GROUP BY dept"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
