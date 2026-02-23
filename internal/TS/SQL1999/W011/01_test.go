package W011

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_W011_RowNumber_L1(t *testing.T) {
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
	} {
		sqlvibeDB.Exec(stmt)
		sqliteDB.Exec(stmt)
	}

	tests := []struct{ name, sql string }{
		{
			"RowNumberOrderBySalary",
			"SELECT id, name, ROW_NUMBER() OVER (ORDER BY salary DESC) AS rn FROM emp ORDER BY rn",
		},
		{
			"RowNumberPartitionByDept",
			"SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn FROM emp ORDER BY dept, rn",
		},
		{
			"RowNumberPartitionOnly",
			"SELECT id, dept, ROW_NUMBER() OVER (PARTITION BY dept) AS rn FROM emp ORDER BY dept, id",
		},
		{
			"RowNumberWholeTable",
			"SELECT id, name, ROW_NUMBER() OVER () AS rn FROM emp ORDER BY id",
		},
		{
			"RowNumberOrderById",
			"SELECT id, name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM emp ORDER BY id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
