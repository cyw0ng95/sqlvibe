package B5_AGGREGATE

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
	_ "github.com/glebarez/go-sqlite"
)

func setup(t *testing.T) (*sqlvibe.Database, *sql.DB) {
	t.Helper()
	sv, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	sl, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return sv, sl
}

// TestSQL1999_B5_CountDistinctNull_L1 tests COUNT with NULLs.
func TestSQL1999_B5_CountDistinctNull_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER, b TEXT)")
	sl.Exec("CREATE TABLE t (a INTEGER, b TEXT)")
	rows := [][]string{{"1", "'x'"}, {"1", "'y'"}, {"NULL", "'z'"}, {"2", "NULL"}, {"2", "'x'"}}
	for _, r := range rows {
		sv.Exec("INSERT INTO t VALUES (" + r[0] + ", " + r[1] + ")")
		sl.Exec("INSERT INTO t VALUES (" + r[0] + ", " + r[1] + ")")
	}

	tests := []struct{ name, sql string }{
		{"CountStar", "SELECT COUNT(*) FROM t"},
		{"CountA", "SELECT COUNT(a) FROM t"},
		{"CountB", "SELECT COUNT(b) FROM t"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B5_SumAvgWithNull_L1 tests SUM/AVG correctly ignoring NULLs.
func TestSQL1999_B5_SumAvgWithNull_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (v REAL)")
	sl.Exec("CREATE TABLE t (v REAL)")
	for _, v := range []string{"1.0", "2.0", "NULL", "3.0", "NULL"} {
		sv.Exec("INSERT INTO t VALUES (" + v + ")")
		sl.Exec("INSERT INTO t VALUES (" + v + ")")
	}

	tests := []struct{ name, sql string }{
		{"SumWithNull", "SELECT SUM(v) FROM t"},
		{"AvgWithNull", "SELECT AVG(v) FROM t"},
		{"CountWithNull", "SELECT COUNT(v) FROM t"},
		{"SumNull", "SELECT SUM(v) FROM t WHERE v IS NULL"},
		{"AvgNull", "SELECT AVG(v) FROM t WHERE v IS NULL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B5_GroupByWithNull_L1 tests GROUP BY with NULL values in group key.
func TestSQL1999_B5_GroupByWithNull_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (grp TEXT, val INTEGER)")
	sl.Exec("CREATE TABLE t (grp TEXT, val INTEGER)")
	rows := [][]string{{"'a'", "1"}, {"'a'", "2"}, {"NULL", "3"}, {"NULL", "4"}, {"'b'", "5"}}
	for _, r := range rows {
		sv.Exec("INSERT INTO t VALUES (" + r[0] + ", " + r[1] + ")")
		sl.Exec("INSERT INTO t VALUES (" + r[0] + ", " + r[1] + ")")
	}

	tests := []struct{ name, sql string }{
		{"GroupByCount", "SELECT grp, COUNT(*) FROM t GROUP BY grp ORDER BY grp"},
		{"GroupBySum", "SELECT grp, SUM(val) FROM t GROUP BY grp ORDER BY grp"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B5_EmptyTable_L1 tests aggregation on empty table.
func TestSQL1999_B5_EmptyTable_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER)")
	sl.Exec("CREATE TABLE t (a INTEGER)")

	tests := []struct{ name, sql string }{
		{"CountEmpty", "SELECT COUNT(*) FROM t"},
		{"SumEmpty", "SELECT SUM(a) FROM t"},
		{"AvgEmpty", "SELECT AVG(a) FROM t"},
		{"MaxEmpty", "SELECT MAX(a) FROM t"},
		{"MinEmpty", "SELECT MIN(a) FROM t"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B5_GroupConcat_L1 tests GROUP_CONCAT function.
func TestSQL1999_B5_GroupConcat_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (grp INTEGER, val TEXT)")
	sl.Exec("CREATE TABLE t (grp INTEGER, val TEXT)")
	rows := [][]string{{"1", "'a'"}, {"1", "'b'"}, {"2", "'c'"}}
	for _, r := range rows {
		sv.Exec("INSERT INTO t VALUES (" + r[0] + ", " + r[1] + ")")
		sl.Exec("INSERT INTO t VALUES (" + r[0] + ", " + r[1] + ")")
	}

	tests := []struct{ name, sql string }{
		{"GroupConcatAll", "SELECT GROUP_CONCAT(val) FROM t"},
		{"GroupConcatSep", "SELECT GROUP_CONCAT(val, '-') FROM t"},
		{"GroupConcatByGroup", "SELECT grp, GROUP_CONCAT(val) FROM t GROUP BY grp ORDER BY grp"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B5_MultiColGroupBy_L1 tests multi-column GROUP BY.
func TestSQL1999_B5_MultiColGroupBy_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (a INTEGER, b TEXT, v INTEGER)")
	sl.Exec("CREATE TABLE t (a INTEGER, b TEXT, v INTEGER)")
	rows := [][]string{
		{"1", "'x'", "10"}, {"1", "'x'", "20"},
		{"1", "'y'", "30"}, {"2", "'x'", "40"},
	}
	for _, r := range rows {
		sv.Exec("INSERT INTO t VALUES (" + r[0] + ", " + r[1] + ", " + r[2] + ")")
		sl.Exec("INSERT INTO t VALUES (" + r[0] + ", " + r[1] + ", " + r[2] + ")")
	}

	tests := []struct{ name, sql string }{
		{"MultiGroupCount", "SELECT a, b, COUNT(*) FROM t GROUP BY a, b ORDER BY a, b"},
		{"MultiGroupSum", "SELECT a, b, SUM(v) FROM t GROUP BY a, b ORDER BY a, b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}
