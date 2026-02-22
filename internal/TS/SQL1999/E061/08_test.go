package E061

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E06108_L1(t *testing.T) {
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
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'apple'), (2, 'banana'), (3, 'orange')"},
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
		{"MATCHSimple", "SELECT * FROM t1 WHERE b MATCH 'apple'"},
		{"MATCHPattern", "SELECT * FROM t1 WHERE b MATCH 'an%na'"},
		{"NOTMATCH", "SELECT * FROM t1 WHERE b NOT MATCH 'apple'"},
		{"MATCHExpression", "SELECT * FROM t1 WHERE b MATCH 'a%'"},
		{"MATCHWithAND", "SELECT * FROM t1 WHERE b MATCH 'a%' AND a > 1"},
		{"MATCHWithOR", "SELECT * FROM t1 WHERE b MATCH 'a%' OR a = 3"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
