package E071

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E07104_L1(t *testing.T) {
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
		{"InsertValues", "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30), (1, 15), (2, 25)"},
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
		{"CorrelatedSubquery", "SELECT * FROM t1 t1_outer WHERE b > (SELECT AVG(b) FROM t1 t1_inner WHERE t1_inner.a = t1_outer.a)"},
		{"CorrelatedSubqueryExists", "SELECT DISTINCT a FROM t1 t1_outer WHERE EXISTS (SELECT 1 FROM t1 t1_inner WHERE t1_inner.a = t1_outer.a AND t1_inner.b > 20)"},
		{"CorrelatedSubqueryNOTExists", "SELECT DISTINCT a FROM t1 t1_outer WHERE NOT EXISTS (SELECT 1 FROM t1 t1_inner WHERE t1_inner.a = t1_outer.a AND t1_inner.b > 100)"},
		{"CorrelatedSubqueryIN", "SELECT * FROM t1 WHERE b IN (SELECT b + 5 FROM t1 t2 WHERE t2.a = t1.a)"},
		{"CorrelatedSubqueryMultipleLevels", "SELECT * FROM t1 t1_outer WHERE b > (SELECT AVG(b) FROM t1 t1_mid WHERE t1_mid.a = (SELECT MIN(a) FROM t1 t1_inner WHERE t1_inner.a = t1_outer.a))"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
