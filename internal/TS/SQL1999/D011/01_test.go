package D011

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_D011_D01101_L1(t *testing.T) {
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
		{"CreateVarcharTable", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, name VARCHAR(100))"},
		{"CreateMultiVarchar", "CREATE TABLE t2 (id INTEGER, first_name VARCHAR(50), last_name VARCHAR(50))"},
		{"CreateVarcharWithDefault", "CREATE TABLE t3 (id INTEGER, status VARCHAR(20) DEFAULT 'active')"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 'Alice')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 'Alice')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 'Bob')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 'Bob')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, NULL)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, NULL)")

	sqlvibeDB.Exec("INSERT INTO t2 VALUES (1, 'John', 'Doe')")
	sqliteDB.Exec("INSERT INTO t2 VALUES (1, 'John', 'Doe')")
	sqlvibeDB.Exec("INSERT INTO t2 VALUES (2, 'Jane', 'Smith')")
	sqliteDB.Exec("INSERT INTO t2 VALUES (2, 'Jane', 'Smith')")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAllVarchar", "SELECT * FROM t1 ORDER BY id"},
		{"SelectWhereVarchar", "SELECT * FROM t1 WHERE name = 'Alice'"},
		{"SelectNullVarchar", "SELECT * FROM t1 WHERE name IS NULL"},
		{"SelectNotNullVarchar", "SELECT * FROM t1 WHERE name IS NOT NULL ORDER BY id"},
		{"SelectConcatVarchar", "SELECT id, first_name || ' ' || last_name AS full_name FROM t2 ORDER BY id"},
		{"SelectUpperVarchar", "SELECT id, upper(name) FROM t1 WHERE name IS NOT NULL ORDER BY id"},
		{"SelectLengthVarchar", "SELECT id, length(name) FROM t1 WHERE name IS NOT NULL ORDER BY id"},
		{"SelectLikeVarchar", "SELECT * FROM t1 WHERE name LIKE 'A%'"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
