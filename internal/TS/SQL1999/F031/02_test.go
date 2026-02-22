package F031

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F03102_L1(t *testing.T) {
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
		{"CreateViewWithAlias", "CREATE VIEW v3 AS SELECT a AS col_a, b AS col_b FROM t1"},
		{"CreateViewWithAggregation", "CREATE VIEW v4 AS SELECT COUNT(*) as cnt FROM t1"},
		{"CreateViewIfNotExists", "CREATE VIEW IF NOT EXISTS v5 AS SELECT a, b FROM t1"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	insertTests := []struct {
		name string
		sql  string
	}{
		{"InsertData", "INSERT INTO t1 VALUES (1, 'test')"},
		{"InsertMultiple", "INSERT INTO t1 VALUES (2, 'data'), (3, 'more')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectFromView", "SELECT * FROM v1"},
		{"SelectFromViewWhere", "SELECT * FROM v2 WHERE a > 5"},
		{"SelectFromViewAlias", "SELECT col_a, col_b FROM v3"},
		{"SelectFromViewAggregation", "SELECT * FROM v4"},
		{"CompareViewVsTable", "SELECT * FROM v1 UNION SELECT a, NULL FROM t1"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
