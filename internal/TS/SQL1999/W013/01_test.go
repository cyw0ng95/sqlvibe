package W013

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_W013_Ntile_L1(t *testing.T) {
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
		"INSERT INTO emp VALUES (7, 'Grace', 'Eng', 88000)",
		"INSERT INTO emp VALUES (8, 'Hank', 'HR', 75000)",
	} {
		sqlvibeDB.Exec(stmt)
		sqliteDB.Exec(stmt)
	}

	tests := []struct{ name, sql string }{
		{
			"Ntile2SplitsIntoTwoGroups",
			"SELECT id, name, salary, NTILE(2) OVER (ORDER BY salary) AS tile FROM emp ORDER BY salary, id",
		},
		{
			"Ntile4SplitsIntoFourGroups",
			"SELECT id, name, salary, NTILE(4) OVER (ORDER BY salary) AS tile FROM emp ORDER BY tile, salary",
		},
		{
			"Ntile2WithPartitionByDept",
			"SELECT id, dept, salary, NTILE(2) OVER (PARTITION BY dept ORDER BY salary) AS tile FROM emp ORDER BY dept, tile, salary",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
