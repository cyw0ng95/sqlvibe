package I011

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_I011_I01101_L1(t *testing.T) {
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

	createTests := []struct {
		name string
		sql  string
	}{
		{"CreateDepts", "CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"},
		{"CreateEmpWithFK", "CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept_id INTEGER, FOREIGN KEY (dept_id) REFERENCES departments(id))"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Insert parent rows first
	sqlvibeDB.Exec("INSERT INTO departments VALUES (1, 'Engineering')")
	sqliteDB.Exec("INSERT INTO departments VALUES (1, 'Engineering')")
	sqlvibeDB.Exec("INSERT INTO departments VALUES (2, 'Marketing')")
	sqliteDB.Exec("INSERT INTO departments VALUES (2, 'Marketing')")

	// Insert child rows with valid FK references
	sqlvibeDB.Exec("INSERT INTO employees VALUES (1, 'Alice', 1)")
	sqliteDB.Exec("INSERT INTO employees VALUES (1, 'Alice', 1)")
	sqlvibeDB.Exec("INSERT INTO employees VALUES (2, 'Bob', 2)")
	sqliteDB.Exec("INSERT INTO employees VALUES (2, 'Bob', 2)")
	sqlvibeDB.Exec("INSERT INTO employees VALUES (3, 'Carol', 1)")
	sqliteDB.Exec("INSERT INTO employees VALUES (3, 'Carol', 1)")
	// NULL FK is allowed
	sqlvibeDB.Exec("INSERT INTO employees VALUES (4, 'Dave', NULL)")
	sqliteDB.Exec("INSERT INTO employees VALUES (4, 'Dave', NULL)")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectDepts", "SELECT * FROM departments ORDER BY id"},
		{"SelectEmps", "SELECT * FROM employees ORDER BY id"},
		{"SelectJoin", "SELECT e.id, e.name, d.name AS dept FROM employees e JOIN departments d ON e.dept_id = d.id ORDER BY e.id"},
		{"SelectNullFK", "SELECT * FROM employees WHERE dept_id IS NULL"},
		{"SelectCountPerDept", "SELECT dept_id, COUNT(*) FROM employees WHERE dept_id IS NOT NULL GROUP BY dept_id ORDER BY dept_id"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
