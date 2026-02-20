package F031

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F03116_L1(t *testing.T) {
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
		{"CreateBaseTable", "CREATE TABLE t1 (a INTEGER, b TEXT)"},
		{"CreateViewSimple", "CREATE VIEW v1 AS SELECT * FROM t1"},
		{"CreateViewWithWhere", "CREATE VIEW v2 AS SELECT * FROM t1 WHERE a > 0"},
		{"CreateMultipleViews", "CREATE VIEW v3 AS SELECT a FROM t1; CREATE VIEW v4 AS SELECT b FROM t1"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	dropTests := []struct {
		name string
		sql  string
	}{
		{"DropSimple", "DROP VIEW v1"},
		{"DropIfNotExists", "DROP VIEW IF EXISTS v1"},
		{"DropMultiple", "DROP VIEW v3; DROP VIEW v4"},
		{"DropWithRESTRICT", "DROP VIEW v2 RESTRICT"},
	}

	for _, tt := range dropTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectFromView", "SELECT * FROM v1"},
		{"QueryViews", "SELECT * FROM sqlite_master WHERE type='view'"},
		{"DropNonExistent", "DROP VIEW IF EXISTS nonexistent"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
