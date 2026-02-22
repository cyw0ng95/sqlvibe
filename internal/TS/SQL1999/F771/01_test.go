package F771

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func setupEmpTable(t *testing.T, svDB *sqlvibe.Database, litDB *sql.DB) {
	t.Helper()
	for _, q := range []string{
		"CREATE TABLE emp (id INTEGER, name TEXT, dept TEXT, salary REAL)",
		"INSERT INTO emp VALUES (1,'Alice','HR',50000)",
		"INSERT INTO emp VALUES (2,'Bob','HR',60000)",
		"INSERT INTO emp VALUES (3,'Carol','IT',70000)",
		"INSERT INTO emp VALUES (4,'Dave','IT',80000)",
		"INSERT INTO emp VALUES (5,'Eve','IT',75000)",
	} {
		svDB.Exec(q)
		litDB.Exec(q)
	}
}

func TestSQL1999_F771_RowNumber_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("sqlvibe: %v", err)
	}
	defer svDB.Close()

	litDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer litDB.Close()

	setupEmpTable(t, svDB, litDB)

	tests := []struct{ name, sql string }{
		// Include salary in SELECT to make outer ORDER BY reliable
		{"RowNumberBasic", "SELECT id, name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM emp ORDER BY id"},
		{"RowNumberBySalary", "SELECT name, salary, ROW_NUMBER() OVER (ORDER BY salary) AS rn FROM emp ORDER BY salary"},
		{"RowNumberPartitioned", "SELECT name, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) AS rn FROM emp ORDER BY dept, salary"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, litDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F771_Rank_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("sqlvibe: %v", err)
	}
	defer svDB.Close()

	litDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer litDB.Close()

	setupEmpTable(t, svDB, litDB)

	tests := []struct{ name, sql string }{
		{"RankBasic", "SELECT name, salary, RANK() OVER (ORDER BY salary) AS rnk FROM emp ORDER BY salary"},
		{"DenseRankBasic", "SELECT name, salary, DENSE_RANK() OVER (ORDER BY salary) AS dr FROM emp ORDER BY salary"},
		{"RankPartitioned", "SELECT name, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY salary) AS rnk FROM emp ORDER BY dept, salary"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, litDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F771_LagLead_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("sqlvibe: %v", err)
	}
	defer svDB.Close()

	litDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer litDB.Close()

	setupEmpTable(t, svDB, litDB)

	tests := []struct{ name, sql string }{
		{"LagBasic", "SELECT id, name, salary, LAG(salary) OVER (ORDER BY id) AS prev_salary FROM emp ORDER BY id"},
		{"LeadBasic", "SELECT id, name, salary, LEAD(salary) OVER (ORDER BY id) AS next_salary FROM emp ORDER BY id"},
		{"LagWithDefault", "SELECT id, name, salary, LAG(salary, 1, 0) OVER (ORDER BY id) AS prev FROM emp ORDER BY id"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, litDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F771_Ntile_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("sqlvibe: %v", err)
	}
	defer svDB.Close()

	litDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer litDB.Close()

	setupEmpTable(t, svDB, litDB)

	tests := []struct{ name, sql string }{
		{"NtileTwo", "SELECT name, salary, NTILE(2) OVER (ORDER BY salary) AS bucket FROM emp ORDER BY salary"},
		{"NtileThree", "SELECT name, salary, NTILE(3) OVER (ORDER BY salary) AS bucket FROM emp ORDER BY salary"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, litDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F771_GroupConcat_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("sqlvibe: %v", err)
	}
	defer svDB.Close()

	litDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer litDB.Close()

	for _, q := range []string{
		"CREATE TABLE items (id INTEGER, cat TEXT, val TEXT)",
		"INSERT INTO items VALUES (1,'A','x')",
		"INSERT INTO items VALUES (2,'A','y')",
		"INSERT INTO items VALUES (3,'B','z')",
	} {
		svDB.Exec(q)
		litDB.Exec(q)
	}

	tests := []struct{ name, sql string }{
		{"GroupConcatBasic", "SELECT cat, GROUP_CONCAT(val) FROM items GROUP BY cat ORDER BY cat"},
		{"GroupConcatSingleGroup", "SELECT GROUP_CONCAT(val) FROM items WHERE cat='A'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, litDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F771_RecursiveCTE_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("sqlvibe: %v", err)
	}
	defer svDB.Close()

	litDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer litDB.Close()

	tests := []struct{ name, sql string }{
		{"CountTo5", "WITH RECURSIVE cnt(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM cnt WHERE n < 5) SELECT n FROM cnt ORDER BY n"},
		{"Fibonacci", "WITH RECURSIVE fib(a,b) AS (SELECT 0,1 UNION ALL SELECT b,a+b FROM fib WHERE a < 10) SELECT a FROM fib ORDER BY a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, litDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F771_CTEColumnList_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("sqlvibe: %v", err)
	}
	defer svDB.Close()

	litDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer litDB.Close()

	tests := []struct{ name, sql string }{
		{"CTEWithColumnList", "WITH nums(n) AS (SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) SELECT n FROM nums ORDER BY n"},
		{"CTEBasicAlias", "WITH named AS (SELECT 42 AS val) SELECT val FROM named"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, litDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F771_WindowFrameSpec_L1(t *testing.T) {
	svDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("sqlvibe: %v", err)
	}
	defer svDB.Close()

	litDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer litDB.Close()

	setupEmpTable(t, svDB, litDB)

	tests := []struct{ name, sql string }{
		// FIRST_VALUE with explicit frame spec parses correctly
		{"FirstValueUnbounded", "SELECT name, dept, salary, FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS first_sal FROM emp ORDER BY dept, salary"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, svDB, litDB, tt.sql, tt.name)
		})
	}
}
