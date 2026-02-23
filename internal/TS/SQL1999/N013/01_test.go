package N013

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_N013_Coalesce_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, a INTEGER, b INTEGER)"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, NULL, 5)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, NULL, 5)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 3, NULL)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 3, NULL)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, NULL, NULL)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, NULL, NULL)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (4, 10, 20)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (4, 10, 20)")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"CoalesceNullVal", "SELECT COALESCE(NULL, 'default')"},
		{"CoalesceNullNullVal", "SELECT COALESCE(NULL, NULL, 'fallback')"},
		{"CoalesceAllNull", "SELECT COALESCE(NULL, NULL, NULL)"},
		{"CoalesceNumbers", "SELECT COALESCE(NULL, 42)"},
		{"CoalesceFirstNonNull", "SELECT COALESCE(1, 2, 3)"},
		{"CoalesceColumns", "SELECT id, COALESCE(a, b) AS result FROM t1 ORDER BY id"},
		{"CoalesceInWhere", "SELECT id FROM t1 WHERE COALESCE(a, b) IS NOT NULL ORDER BY id"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
