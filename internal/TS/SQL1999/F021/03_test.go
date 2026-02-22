package F021

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F02103_L1(t *testing.T) {
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
		{"CreateViewSimple", "CREATE VIEW v1 AS SELECT a FROM t1"},
		{"CreateViewWithWhere", "CREATE VIEW v2 AS SELECT * FROM t1 WHERE a > 0"},
		{"CreateViewWithJoin", "CREATE VIEW v3 AS SELECT t1.a, t1.b FROM t1"},
		{"CreateViewWithAggregation", "CREATE VIEW v4 AS SELECT COUNT(*) as cnt FROM t1"},
		{"CreateViewMultiple", "CREATE VIEW v5 AS SELECT a, b FROM t1; CREATE VIEW v6 AS SELECT b FROM t1"},
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
		{"QueryAllViews", "SELECT * FROM information_schema.views"},
		{"QueryByViewName", "SELECT * FROM information_schema.views WHERE table_name = 'v1'"},
		{"QueryBySchema", "SELECT * FROM information_schema.views WHERE table_schema = 'main'"},
		{"QueryViewNames", "SELECT table_name FROM information_schema.views ORDER BY table_name"},
		{"QueryViewDefinition", "SELECT table_name, view_definition FROM information_schema.views"},
		{"QueryMultipleViews", "SELECT * FROM information_schema.views WHERE table_schema = 'main' ORDER BY table_name"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			rows := SQL1999.QuerySqlvibeOnly(t, sqlvibeDB, tt.sql, tt.name)
			if rows == nil {
				return
			}
			// Verify query succeeds for information_schema
		})
	}
}
