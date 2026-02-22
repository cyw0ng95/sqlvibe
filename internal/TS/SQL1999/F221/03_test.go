package F221

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F22103_L1(t *testing.T) {
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

	tests := []struct {
		name string
		sql  string
	}{
		{"IntDefault", "CREATE TABLE t1 (id INTEGER, score INTEGER DEFAULT 0)"},
		{"TextDefault", "CREATE TABLE t2 (id INTEGER, status TEXT DEFAULT 'active')"},
		{"RealDefault", "CREATE TABLE t3 (id INTEGER, price REAL DEFAULT 0.0)"},
		{"NullDefault", "CREATE TABLE t4 (id INTEGER, note TEXT DEFAULT NULL)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO t1 (id) VALUES (1)")
	sqliteDB.Exec("INSERT INTO t1 (id) VALUES (1)")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t1", "VerifyIntDefault")

	sqlvibeDB.Exec("INSERT INTO t2 (id) VALUES (1)")
	sqliteDB.Exec("INSERT INTO t2 (id) VALUES (1)")
	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM t2", "VerifyTextDefault")
}
