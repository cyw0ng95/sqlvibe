package W016

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_W016_AggregateWindowFunctions_L1(t *testing.T) {
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
			"SumRunningTotal",
			"SELECT id, name, salary, SUM(salary) OVER (ORDER BY salary) AS running_total FROM emp ORDER BY salary",
		},
		{
			"AvgRunningAverage",
			"SELECT id, name, salary, AVG(salary) OVER (ORDER BY salary) AS running_avg FROM emp ORDER BY salary",
		},
		{
			"MinOverPartition",
			"SELECT id, dept, salary, MIN(salary) OVER (PARTITION BY dept) AS dept_min FROM emp ORDER BY dept, salary",
		},
		{
			"MaxOverPartition",
			"SELECT id, dept, salary, MAX(salary) OVER (PARTITION BY dept) AS dept_max FROM emp ORDER BY dept, salary",
		},
		{
			"SumOverPartitionByDept",
			"SELECT id, dept, salary, SUM(salary) OVER (PARTITION BY dept ORDER BY salary) AS dept_running FROM emp ORDER BY dept, salary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

