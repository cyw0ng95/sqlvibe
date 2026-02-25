package D012

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_D012_D01201_L1(t *testing.T) {
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
		{"CreateCharTable", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, code CHAR(10))"},
		{"CreateCharWithDefault", "CREATE TABLE t2 (id INTEGER, flag CHAR(1) DEFAULT 'Y')"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 'ABC')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 'ABC')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 'XYZ')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 'XYZ')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, NULL)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, NULL)")

	sqlvibeDB.Exec("INSERT INTO t2 VALUES (1, 'Y')")
	sqliteDB.Exec("INSERT INTO t2 VALUES (1, 'Y')")
	sqlvibeDB.Exec("INSERT INTO t2 VALUES (2, 'N')")
	sqliteDB.Exec("INSERT INTO t2 VALUES (2, 'N')")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAllChar", "SELECT * FROM t1 ORDER BY id"},
		{"SelectWhereChar", "SELECT * FROM t1 WHERE code = 'ABC'"},
		{"SelectNullChar", "SELECT * FROM t1 WHERE code IS NULL"},
		{"SelectNotNullChar", "SELECT * FROM t1 WHERE code IS NOT NULL ORDER BY id"},
		{"SelectCharLength", "SELECT id, length(code) FROM t1 WHERE code IS NOT NULL ORDER BY id"},
		{"SelectCharUpper", "SELECT id, upper(code) FROM t1 WHERE code IS NOT NULL ORDER BY id"},
		{"SelectCharFlag", "SELECT * FROM t2 WHERE flag = 'Y'"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
