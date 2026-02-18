package E071

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E07103_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b INTEGER)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)"},
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
		{"SubqueryInFROM", "SELECT * FROM (SELECT * FROM t1) AS sub"},
		{"SubqueryInFROMWithAlias", "SELECT t.* FROM (SELECT * FROM t1) AS t"},
		{"SubqueryInFROMWithCols", "SELECT s.a, s.b FROM (SELECT a, b FROM t1) AS s"},
		{"SubqueryInFROMWithFilter", "SELECT * FROM (SELECT * FROM t1 WHERE a > 1) AS s"},
		{"SubqueryInFROMWithAgg", "SELECT * FROM (SELECT a, COUNT(*) AS cnt FROM t1 GROUP BY a) AS s"},
		{"MultipleSubqueriesInFROM", "SELECT s1.a, s2.b FROM (SELECT * FROM t1) AS s1 JOIN (SELECT * FROM t1) AS s2 ON s1.a = s2.a"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
