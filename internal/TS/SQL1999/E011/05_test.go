package E011

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E01105_L1(t *testing.T) {
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
		{"CreateIntegers", "CREATE TABLE ints (id INTEGER PRIMARY KEY, val INTEGER)"},
		{"CreateReals", "CREATE TABLE reals (id INTEGER PRIMARY KEY, val REAL)"},
		{"CreateMixed", "CREATE TABLE mixed (id INTEGER PRIMARY KEY, i_val INTEGER, r_val REAL)"},
		{"CreateWithNulls", "CREATE TABLE with_nulls (id INTEGER PRIMARY KEY, val INTEGER)"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("INSERT INTO ints VALUES (1, 10), (2, 20), (3, 30), (4, 20), (5, 10)")
	sqliteDB.Exec("INSERT INTO ints VALUES (1, 10), (2, 20), (3, 30), (4, 20), (5, 10)")

	sqlvibeDB.Exec("INSERT INTO reals VALUES (1, 10.5), (2, 20.5), (3, 30.5), (4, 20.5), (5, 10.5)")
	sqliteDB.Exec("INSERT INTO reals VALUES (1, 10.5), (2, 20.5), (3, 30.5), (4, 20.5), (5, 10.5)")

	sqlvibeDB.Exec("INSERT INTO mixed VALUES (1, 10, 10.5), (2, 20, 20.5), (3, 30, 30.5)")
	sqliteDB.Exec("INSERT INTO mixed VALUES (1, 10, 10.5), (2, 20, 20.5), (3, 30, 30.5)")

	sqlvibeDB.Exec("INSERT INTO with_nulls VALUES (1, 10), (2, NULL), (3, 20)")
	sqliteDB.Exec("INSERT INTO with_nulls VALUES (1, 10), (2, NULL), (3, 20)")

	equalityTests := []struct {
		name string
		sql  string
	}{
		{"Equal_10", "SELECT val = 10 FROM ints ORDER BY id"},
		{"Equal_20", "SELECT val = 20 FROM ints ORDER BY id"},
		{"Equal_30", "SELECT val = 30 FROM ints ORDER BY id"},
		{"NotEqual_10", "SELECT val <> 10 FROM ints ORDER BY id"},
		{"NotEqual_20", "SELECT val <> 20 FROM ints ORDER BY id"},
		{"Equal_Reals", "SELECT val = 10.5 FROM reals ORDER BY id"},
		{"NotEqual_Reals", "SELECT val <> 10.5 FROM reals ORDER BY id"},
		{"Equal_Column", "SELECT i_val = r_val FROM mixed ORDER BY id"},
		{"NotEqual_Column", "SELECT i_val <> r_val FROM mixed ORDER BY id"},
	}

	for _, tt := range equalityTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	greaterThanTests := []struct {
		name string
		sql  string
	}{
		{"GreaterThan_15", "SELECT val > 15 FROM ints ORDER BY id"},
		{"GreaterThan_20", "SELECT val > 20 FROM ints ORDER BY id"},
		{"GreaterThan_10", "SELECT val > 10 FROM ints ORDER BY id"},
		{"GreaterOrEqual_15", "SELECT val >= 15 FROM ints ORDER BY id"},
		{"GreaterOrEqual_20", "SELECT val >= 20 FROM ints ORDER BY id"},
		{"GreaterOrEqual_10", "SELECT val >= 10 FROM ints ORDER BY id"},
		{"GreaterThan_Reals", "SELECT val > 15.5 FROM reals ORDER BY id"},
		{"GreaterOrEqual_Reals", "SELECT val >= 20.5 FROM reals ORDER BY id"},
		{"GreaterThan_Column", "SELECT i_val > 15 FROM mixed ORDER BY id"},
		{"GreaterOrEqual_Column", "SELECT i_val >= 20 FROM mixed ORDER BY id"},
	}

	for _, tt := range greaterThanTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	lessThanTests := []struct {
		name string
		sql  string
	}{
		{"LessThan_25", "SELECT val < 25 FROM ints ORDER BY id"},
		{"LessThan_20", "SELECT val < 20 FROM ints ORDER BY id"},
		{"LessThan_10", "SELECT val < 10 FROM ints ORDER BY id"},
		{"LessOrEqual_25", "SELECT val <= 25 FROM ints ORDER BY id"},
		{"LessOrEqual_20", "SELECT val <= 20 FROM ints ORDER BY id"},
		{"LessOrEqual_10", "SELECT val <= 10 FROM ints ORDER BY id"},
		{"LessThan_Reals", "SELECT val < 25.5 FROM reals ORDER BY id"},
		{"LessOrEqual_Reals", "SELECT val <= 20.5 FROM reals ORDER BY id"},
		{"LessThan_Column", "SELECT i_val < 25 FROM mixed ORDER BY id"},
		{"LessOrEqual_Column", "SELECT i_val <= 20 FROM mixed ORDER BY id"},
	}

	for _, tt := range lessThanTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	whereClauseTests := []struct {
		name string
		sql  string
	}{
		{"Where_Equal", "SELECT * FROM ints WHERE val = 20 ORDER BY id"},
		{"Where_NotEqual", "SELECT * FROM ints WHERE val <> 20 ORDER BY id"},
		{"Where_Greater", "SELECT * FROM ints WHERE val > 15 ORDER BY id"},
		{"Where_GreaterOrEqual", "SELECT * FROM ints WHERE val >= 20 ORDER BY id"},
		{"Where_Less", "SELECT * FROM ints WHERE val < 30 ORDER BY id"},
		{"Where_LessOrEqual", "SELECT * FROM ints WHERE val <= 20 ORDER BY id"},
		{"Where_Combine_AND", "SELECT * FROM ints WHERE val >= 15 AND val <= 25 ORDER BY id"},
		{"Where_Combine_OR", "SELECT * FROM ints WHERE val = 10 OR val = 30 ORDER BY id"},
		{"Where_Combine_NOT", "SELECT * FROM ints WHERE NOT (val = 20) ORDER BY id"},
	}

	for _, tt := range whereClauseTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	orderByTests := []struct {
		name string
		sql  string
	}{
		{"OrderBy_ASC", "SELECT val FROM ints ORDER BY val ASC"},
		{"OrderBy_DESC", "SELECT val FROM ints ORDER BY val DESC"},
		{"OrderBy_GreaterThan", "SELECT val FROM ints WHERE val > 10 ORDER BY val"},
		{"OrderBy_LessThan", "SELECT val FROM ints WHERE val < 30 ORDER BY val DESC"},
		{"OrderBy_Expression", "SELECT val FROM ints ORDER BY val * -1"},
	}

	for _, tt := range orderByTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	nullComparisonTests := []struct {
		name string
		sql  string
	}{
		{"NULL_Equal", "SELECT val = 10 FROM with_nulls ORDER BY id"},
		{"NULL_NotEqual", "SELECT val <> 10 FROM with_nulls ORDER BY id"},
		{"NULL_Greater", "SELECT val > 10 FROM with_nulls ORDER BY id"},
		{"NULL_GreaterOrEqual", "SELECT val >= 10 FROM with_nulls ORDER BY id"},
		{"NULL_Less", "SELECT val < 10 FROM with_nulls ORDER BY id"},
		{"NULL_LessOrEqual", "SELECT val <= 10 FROM with_nulls ORDER BY id"},
		{"NULL_IS_NULL", "SELECT val IS NULL FROM with_nulls ORDER BY id"},
		{"NULL_IS_NOT_NULL", "SELECT val IS NOT NULL FROM with_nulls ORDER BY id"},
	}

	for _, tt := range nullComparisonTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	mixedTypeTests := []struct {
		name string
		sql  string
	}{
		{"Mixed_Integer_Real_Equal", "SELECT i_val = r_val FROM mixed ORDER BY id"},
		{"Mixed_Integer_Real_Greater", "SELECT i_val > 15.0 FROM mixed ORDER BY id"},
		{"Mixed_Integer_Real_Less", "SELECT r_val < 25 FROM mixed ORDER BY id"},
	}

	for _, tt := range mixedTypeTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
