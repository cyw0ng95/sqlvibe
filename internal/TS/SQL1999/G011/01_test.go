package G011

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_G011_G01101_L1(t *testing.T) {
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
		{"CreateSimpleTable", "CREATE TABLE t1 (id INTEGER)"},
		{"CreateMultiCol", "CREATE TABLE t2 (id INTEGER, name TEXT, val REAL, flag INTEGER)"},
		{"CreateWithPK", "CREATE TABLE t3 (id INTEGER PRIMARY KEY, name TEXT)"},
		{"CreateWithNotNull", "CREATE TABLE t4 (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER)"},
		{"CreateWithDefault", "CREATE TABLE t5 (id INTEGER PRIMARY KEY, status TEXT DEFAULT 'pending', score INTEGER DEFAULT 0)"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO t2 VALUES (1, 'Alice', 3.14, 1)")
	sqliteDB.Exec("INSERT INTO t2 VALUES (1, 'Alice', 3.14, 1)")
	sqlvibeDB.Exec("INSERT INTO t2 VALUES (2, 'Bob', 2.71, 0)")
	sqliteDB.Exec("INSERT INTO t2 VALUES (2, 'Bob', 2.71, 0)")

	sqlvibeDB.Exec("INSERT INTO t5 (id, score) VALUES (1, 42)")
	sqliteDB.Exec("INSERT INTO t5 (id, score) VALUES (1, 42)")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectFromT2", "SELECT * FROM t2 ORDER BY id"},
		{"SelectColsFromT2", "SELECT id, name FROM t2 ORDER BY id"},
		{"SelectWithDefault", "SELECT id, status, score FROM t5"},
		{"SelectCount", "SELECT COUNT(*) FROM t2"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	dropTests := []struct {
		name string
		sql  string
	}{
		{"DropT1", "DROP TABLE t1"},
		{"DropT2", "DROP TABLE t2"},
	}
	for _, tt := range dropTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
