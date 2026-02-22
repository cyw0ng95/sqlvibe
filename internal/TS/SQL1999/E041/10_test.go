package E041

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E04110_L1(t *testing.T) {
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
		{"CreateBaseTable", "CREATE TABLE t1 (a INTEGER, b TEXT, c REAL)"},
		{"CreateSimpleView", "CREATE VIEW v1 AS SELECT a, b FROM t1"},
		{"CreateViewWithFilter", "CREATE VIEW v2 AS SELECT * FROM t1 WHERE c > 5.0"},
		{"CreateViewWithOrder", "CREATE VIEW v3 AS SELECT a, b FROM t1 ORDER BY a"},
		{"CreateViewWithExpression", "CREATE VIEW v4 AS SELECT a * 2 AS double_a, b FROM t1"},
		{"CreateViewWithJoin", "CREATE TABLE t2 (x INTEGER, y TEXT)"},
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
		{"InsertIntoT1", "INSERT INTO t1 VALUES (1, 'test1', 3.14), (2, 'test2', 6.28), (3, 'test3', 9.42)"},
		{"InsertIntoT2", "INSERT INTO t2 VALUES (1, 'a'), (2, 'b'), (3, 'c')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	createViewWithJoinTests := []struct {
		name string
		sql  string
	}{
		{"CreateViewWithJoin", "CREATE VIEW v5 AS SELECT t1.a, t2.y FROM t1 JOIN t2 ON t1.a = t2.x"},
	}

	for _, tt := range createViewWithJoinTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	createIfNotExistsTests := []struct {
		name string
		sql  string
	}{
		{"CreateViewIfExists", "CREATE VIEW v1 AS SELECT a, b FROM t1"},
		{"CreateViewIfNotExists", "CREATE VIEW IF NOT EXISTS v1 AS SELECT a, b FROM t1"},
		{"CreateNewViewIfNotExists", "CREATE VIEW IF NOT EXISTS v6 AS SELECT * FROM t2"},
	}

	for _, tt := range createIfNotExistsTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectFromV1", "SELECT * FROM v1"},
		{"SelectFromV2", "SELECT * FROM v2"},
		{"SelectFromV3", "SELECT * FROM v3"},
		{"SelectFromV4", "SELECT * FROM v4"},
		{"SelectFromV5", "SELECT * FROM v5"},
		{"SelectFromV6", "SELECT * FROM v6"},
		{"CheckViewsExist", "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
