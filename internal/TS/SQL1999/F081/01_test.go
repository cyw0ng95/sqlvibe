package F081

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F801_F08101_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateTable1", "CREATE TABLE t1 (id INTEGER, val INTEGER)"},
		{"CreateTable2", "CREATE TABLE t2 (id INTEGER, val INTEGER)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30), (4, 40)"},
		{"InsertT2", "INSERT INTO t2 VALUES (5, 50), (6, 60), (7, 70), (8, 80)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	unionTests := []struct {
		name string
		sql  string
	}{
		{"UnionAllBasic", "SELECT id, val FROM t1 UNION ALL SELECT id, val FROM t2"},
		{"UnionDistinct", "SELECT id, val FROM t1 UNION SELECT id, val FROM t2"},
		{"UnionDistinctImplicit", "SELECT id, val FROM t1 UNION ALL SELECT id, val FROM t2 ORDER BY val"},
		{"UnionAllSameTable", "SELECT id, val FROM t1 UNION ALL SELECT id, val FROM t1"},
		{"UnionDistinctSameTable", "SELECT DISTINCT id, val FROM t1 UNION SELECT DISTINCT id, val FROM t1"},
		{"UnionImplicit", "SELECT id, val FROM t1 UNION ALL SELECT id, val FROM t2 ORDER BY val"},
		{"UnionMixed", "SELECT id, val FROM t1 UNION DISTINCT SELECT id, val FROM t2 ORDER BY val"},
	}

	for _, tt := range unionTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
