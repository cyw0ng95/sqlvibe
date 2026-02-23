package D016

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_D016_D01601_L1(t *testing.T) {
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
		{"CreateBlobTable", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, data BLOB, name TEXT)"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, X'48656C6C6F', 'hello')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, X'48656C6C6F', 'hello')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, X'DEADBEEF', 'deadbeef')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, X'DEADBEEF', 'deadbeef')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, NULL, 'nullblob')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, NULL, 'nullblob')")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT id, name FROM t1 ORDER BY id"},
		{"SelectHex", "SELECT id, hex(data) FROM t1 WHERE data IS NOT NULL ORDER BY id"},
		{"SelectBlobLength", "SELECT id, length(data) FROM t1 ORDER BY id"},
		{"SelectNullBlob", "SELECT * FROM t1 WHERE data IS NULL"},
		{"SelectNotNullBlob", "SELECT id, name FROM t1 WHERE data IS NOT NULL ORDER BY id"},
		{"SelectZeroblob", "SELECT hex(zeroblob(3))"},
		{"SelectHexLiteral", "SELECT hex(X'CAFE')"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
