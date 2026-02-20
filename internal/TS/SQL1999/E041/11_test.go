package E041

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E04111_L1(t *testing.T) {
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
		{"CreateView1", "CREATE VIEW v1 AS SELECT a, b FROM t1"},
		{"CreateView2", "CREATE VIEW v2 AS SELECT * FROM t1 WHERE c > 5.0"},
		{"CreateView3", "CREATE VIEW v3 AS SELECT a * 2 AS double_a, b FROM t1"},
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
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryBeforeDrop := []struct {
		name string
		sql  string
	}{
		{"SelectFromV1Before", "SELECT * FROM v1"},
		{"SelectFromV2Before", "SELECT * FROM v2"},
		{"CheckViewsBeforeDrop", "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"},
	}

	for _, tt := range queryBeforeDrop {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	dropTests := []struct {
		name string
		sql  string
	}{
		{"DropView1", "DROP VIEW v1"},
		{"DropView2", "DROP VIEW v2"},
	}

	for _, tt := range dropTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	dropIfExistsTests := []struct {
		name string
		sql  string
	}{
		{"DropIfExistsExisting", "DROP VIEW IF EXISTS v3"},
		{"DropIfExistsNonExisting", "DROP VIEW IF EXISTS non_existent_view"},
	}

	for _, tt := range dropIfExistsTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryAfterDrop := []struct {
		name string
		sql  string
	}{
		{"CheckViewsAfterDrop", "SELECT name FROM sqlite_master WHERE type='view' ORDER BY name"},
		{"SelectFromT1", "SELECT * FROM t1 ORDER BY a"},
	}

	for _, tt := range queryAfterDrop {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
