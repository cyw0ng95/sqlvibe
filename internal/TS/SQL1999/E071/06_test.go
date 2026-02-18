package E071

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E07106_L1(t *testing.T) {
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
		{"NestedSubqueryInSelect", "SELECT a, (SELECT b FROM (SELECT b FROM t1 WHERE a = t1_outer.a LIMIT 1)) AS b FROM t1 AS t1_outer"},
		{"NestedSubqueryInWhere", "SELECT * FROM t1 WHERE a IN (SELECT a FROM (SELECT a FROM t1 WHERE b > 10) AS sub)"},
		{"NestedSubqueryInFROM", "SELECT * FROM (SELECT * FROM (SELECT * FROM t1 WHERE a > 1) AS s1) AS s2"},
		{"NestedCorrelatedSubquery", "SELECT * FROM t1 t1_outer WHERE b > (SELECT AVG(b) FROM t1 t1_mid WHERE t1_mid.a = (SELECT a FROM t1 t1_inner WHERE t1_inner.a = t1_outer.a))"},
		{"TripleNestedSubquery", "SELECT * FROM t1 WHERE a IN (SELECT a FROM (SELECT a FROM (SELECT a FROM t1 WHERE b > 10) AS s1) AS s2)"},
		{"NestedSubqueryWithAggregation", "SELECT * FROM t1 WHERE b > (SELECT MAX(avg_b) FROM (SELECT AVG(b) AS avg_b FROM t1 GROUP BY a) AS sub)"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
