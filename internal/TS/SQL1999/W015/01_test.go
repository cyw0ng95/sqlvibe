package W015

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_W015_FirstLastValue_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	for _, stmt := range []string{
		"CREATE TABLE emp (id INTEGER, name TEXT, dept TEXT, salary INTEGER)",
		"INSERT INTO emp VALUES (1, 'Alice', 'Eng', 90000)",
		"INSERT INTO emp VALUES (2, 'Bob', 'Eng', 85000)",
		"INSERT INTO emp VALUES (3, 'Carol', 'HR', 70000)",
		"INSERT INTO emp VALUES (4, 'Dave', 'HR', 72000)",
		"INSERT INTO emp VALUES (5, 'Eve', 'Eng', 95000)",
		"INSERT INTO emp VALUES (6, 'Frank', 'HR', 68000)",
	} {
		sqlvibeDB.Exec(stmt)
		sqliteDB.Exec(stmt)
	}

	tests := []struct{ name, sql string }{
		{
			"FirstValuePartitionBySalary",
			"SELECT id, dept, salary, FIRST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary) AS lowest_earner FROM emp ORDER BY dept, salary",
		},
		// LAST_VALUE without an explicit frame uses the default frame (RANGE BETWEEN UNBOUNDED
		// PRECEDING AND CURRENT ROW), so it returns the current row's value in both engines.
		{
			"LastValueDefaultFramePartition",
			"SELECT id, dept, salary, LAST_VALUE(name) OVER (PARTITION BY dept ORDER BY salary) AS cur_name FROM emp ORDER BY dept, salary",
		},
		{
			"FirstValueOrderBySalaryDesc",
			"SELECT id, name, salary, FIRST_VALUE(name) OVER (ORDER BY salary DESC) AS top_earner FROM emp ORDER BY salary DESC",
		},
		{
			"LastValueDefaultFrameWholeTable",
			"SELECT id, name, salary, LAST_VALUE(name) OVER (ORDER BY salary) AS cur_name FROM emp ORDER BY salary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
