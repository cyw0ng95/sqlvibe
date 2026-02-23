package R011

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_R011_R01101_L1(t *testing.T) {
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
		{"CreateIntegerTable", "CREATE TABLE t_int (id INTEGER, val INTEGER)"},
		{"CreateTextTable", "CREATE TABLE t_text (id INTEGER, val TEXT)"},
		{"CreateRealTable", "CREATE TABLE t_real (id INTEGER, val REAL)"},
		{"CreateBlobTable", "CREATE TABLE t_blob (id INTEGER, val BLOB)"},
		{"CreateIfNotExists", "CREATE TABLE IF NOT EXISTS t_int (id INTEGER, val INTEGER)"},
		{"CreateWithPrimaryKey", "CREATE TABLE t_pk (id INTEGER PRIMARY KEY, name TEXT)"},
		{"CreateWithDefault", "CREATE TABLE t_default (id INTEGER, status TEXT DEFAULT 'active', score REAL DEFAULT 0.0)"},
		{"CreateWithNotNull", "CREATE TABLE t_notnull (id INTEGER NOT NULL, name TEXT NOT NULL)"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO t_pk VALUES (1, 'Alice')")
	sqliteDB.Exec("INSERT INTO t_pk VALUES (1, 'Alice')")
	sqlvibeDB.Exec("INSERT INTO t_pk VALUES (2, 'Bob')")
	sqliteDB.Exec("INSERT INTO t_pk VALUES (2, 'Bob')")

	sqlvibeDB.Exec("INSERT INTO t_notnull VALUES (1, 'hello')")
	sqliteDB.Exec("INSERT INTO t_notnull VALUES (1, 'hello')")
	sqlvibeDB.Exec("INSERT INTO t_notnull VALUES (2, 'world')")
	sqliteDB.Exec("INSERT INTO t_notnull VALUES (2, 'world')")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectPK", "SELECT * FROM t_pk ORDER BY id"},
		{"SelectNotNull", "SELECT * FROM t_notnull ORDER BY id"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
