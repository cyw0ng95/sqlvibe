package E061

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E06106_L1(t *testing.T) {
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
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'test')"},
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
		{"EXISTS", "SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t1)"},
		{"NOTEXISTS", "SELECT * FROM t1 WHERE NOT EXISTS (SELECT * FROM t1 WHERE a > 10)"},
		{"EXISTSWithCondition", "SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t1 WHERE a = 1)"},
		{"NOTEXISTSWithCondition", "SELECT * FROM t1 WHERE NOT EXISTS (SELECT * FROM t1 WHERE a = 10)"},
		{"EXISTSWithSubquery", "SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t1 WHERE a = t1.a)"},
		{"EXISTSWithJoin", "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t1 t2 WHERE t2.a = t1.a + 1)"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
