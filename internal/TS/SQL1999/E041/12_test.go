package E041

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E04112_L1(t *testing.T) {
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
		{"CreateBaseTable", "CREATE TABLE t1 (a INTEGER, b TEXT, c REAL)"},
		{"InsertIntoBase", "INSERT INTO t1 VALUES (1, 'test1', 3.14), (2, 'test2', 6.28), (3, 'test3', 9.42)"},
		{"CreateTableAsSelectAll", "CREATE TABLE t2 AS SELECT * FROM t1"},
		{"CreateTableAsSelectCols", "CREATE TABLE t3 AS SELECT a, b FROM t1"},
		{"CreateTableAsSelectFilter", "CREATE TABLE t4 AS SELECT * FROM t1 WHERE c > 5.0"},
		{"CreateTableAsSelectExpr", "CREATE TABLE t5 AS SELECT a * 2 AS double_a, b FROM t1"},
		{"CreateTableAsSelectOrder", "CREATE TABLE t6 AS SELECT * FROM t1 ORDER BY a"},
		{"CreateTableAsSelectDistinct", "CREATE TABLE t7 AS SELECT DISTINCT b FROM t1"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectFromT1", "SELECT * FROM t1 ORDER BY a"},
		{"SelectFromT2", "SELECT * FROM t2 ORDER BY a"},
		{"SelectFromT3", "SELECT * FROM t3 ORDER BY a"},
		{"SelectFromT4", "SELECT * FROM t4 ORDER BY a"},
		{"SelectFromT5", "SELECT * FROM t5 ORDER BY double_a"},
		{"SelectFromT6", "SELECT * FROM t6 ORDER BY a"},
		{"SelectFromT7", "SELECT * FROM t7 ORDER BY b"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	schemaTests := []struct {
		name string
		sql  string
	}{
		{"CheckT1Schema", "PRAGMA table_info(t1)"},
		{"CheckT2Schema", "PRAGMA table_info(t2)"},
		{"CheckT3Schema", "PRAGMA table_info(t3)"},
		{"CheckT4Schema", "PRAGMA table_info(t4)"},
		{"CheckT5Schema", "PRAGMA table_info(t5)"},
		{"CheckT6Schema", "PRAGMA table_info(t6)"},
		{"CheckT7Schema", "PRAGMA table_info(t7)"},
	}

	for _, tt := range schemaTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
