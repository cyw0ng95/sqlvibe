package E071

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E07105_L1(t *testing.T) {
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
		{"InsertValues", "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30), (4, 40)"},
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
		{"SubqueryALL", "SELECT * FROM t1 WHERE b > ALL (SELECT b FROM t1 WHERE a < 3)"},
		{"SubqueryANY", "SELECT * FROM t1 WHERE b > ANY (SELECT b FROM t1 WHERE a < 3)"},
		{"SubquerySOME", "SELECT * FROM t1 WHERE b > SOME (SELECT b FROM t1 WHERE a < 3)"},
		{"SubqueryNotALL", "SELECT * FROM t1 WHERE b NOT > ALL (SELECT b FROM t1 WHERE a < 3)"},
		{"SubqueryNOTANY", "SELECT * FROM t1 WHERE b NOT > ANY (SELECT b FROM t1 WHERE a < 3)"},
		{"SubqueryCorrelatedALL", "SELECT * FROM t1 t1_outer WHERE b > ALL (SELECT b FROM t1 t1_inner WHERE t1_inner.a < t1_outer.a)"},
		{"SubqueryCorrelatedANY", "SELECT * FROM t1 t1_outer WHERE b > ANY (SELECT b FROM t1 t1_inner WHERE t1_inner.a < t1_outer.a)"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
