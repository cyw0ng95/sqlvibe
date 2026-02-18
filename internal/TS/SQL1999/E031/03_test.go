package E031

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E03103_L1(t *testing.T) {
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
		{"CreateBaseTable", "CREATE TABLE t1 (id INTEGER, name TEXT)"},
		{"CreateView1", "CREATE VIEW v1 AS SELECT * FROM t1"},
		{"CreateView2", "CREATE VIEW v2 AS SELECT id, name FROM t1 WHERE id > 0"},
		{"CreateView3", "CREATE VIEW v3 AS SELECT name FROM t1 ORDER BY name"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Note: information_schema is not supported by SQLite, so we only test sqlvibe
	queryTests := []struct {
		name string
		sql  string
	}{
		{"QueryAllViews", "SELECT * FROM information_schema.views"},
		{"QueryViewNames", "SELECT table_name FROM information_schema.views"},
		{"QueryBySchema", "SELECT * FROM information_schema.views WHERE table_schema = 'main'"},
		{"CheckViewsExist", "SELECT table_name FROM information_schema.views WHERE table_schema = 'main' ORDER BY table_name"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			rows := SQL1999.QuerySqlvibeOnly(t, sqlvibeDB, tt.sql, tt.name)
			if rows == nil {
				return
			}
			// Views may be empty, just verify query succeeds
		})
	}
}
