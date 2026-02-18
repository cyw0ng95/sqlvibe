package E061

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E06107_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b TEXT)"},
		{"InsertUniqueValues", "INSERT INTO t1 VALUES (1, 'test'), (2, 'hello'), (3, 'world')"},
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
		{"UNIQUE", "SELECT * FROM t1 WHERE a = 1"},
		{"UNIQUEWithSubquery", "SELECT * FROM t1 WHERE UNIQUE (SELECT a FROM t1 WHERE a = t1.a)"},
		{"UNIQUEMultiple", "SELECT * FROM t1 WHERE UNIQUE (SELECT a FROM t1 WHERE a > t1.a)"},
		{"UNIQUEWithJoin", "SELECT * FROM t1 WHERE UNIQUE (SELECT t2.a FROM t1 t2 WHERE t2.a = t1.a + 1)"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
