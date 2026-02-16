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

	sqlvibeDB.Exec("CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)")
	sqliteDB.Exec("CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER)")

	sqlvibeDB.Exec("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30), (4, 20), (5, 10), (6, NULL), (7, -5), (8, 0)")
	sqliteDB.Exec("INSERT INTO nums VALUES (1, 10), (2, 20), (3, 30), (4, 20), (5, 10), (6, NULL), (7, -5), (8, 0)")

	equalityTests := []struct {
		name string
		sql  string
	}{
		{"Equal_10", "SELECT val = 10 FROM nums ORDER BY id"},
		{"Equal_20", "SELECT val = 20 FROM nums ORDER BY id"},
		{"Equal_30", "SELECT val = 30 FROM nums ORDER BY id"},
		{"Equal_0", "SELECT val = 0 FROM nums ORDER BY id"},
		{"Equal_Neg5", "SELECT val = -5 FROM nums ORDER BY id"},
		{"NotEqual_10", "SELECT val <> 10 FROM nums ORDER BY id"},
		{"NotEqual_20", "SELECT val <> 20 FROM nums ORDER BY id"},
		{"NotEqual_0", "SELECT val <> 0 FROM nums ORDER BY id"},
		{"NotEqual_Neg5", "SELECT val <> -5 FROM nums ORDER BY id"},
		{"BangEqual_10", "SELECT val != 10 FROM nums ORDER BY id"},
		{"BangEqual_20", "SELECT val != 20 FROM nums ORDER BY id"},
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
		{"GreaterThan_15", "SELECT val > 15 FROM nums ORDER BY id"},
		{"GreaterThan_20", "SELECT val > 20 FROM nums ORDER BY id"},
		{"GreaterThan_10", "SELECT val > 10 FROM nums ORDER BY id"},
		{"GreaterThan_0", "SELECT val > 0 FROM nums ORDER BY id"},
		{"GreaterThan_Neg10", "SELECT val > -10 FROM nums ORDER BY id"},
		{"GreaterOrEqual_15", "SELECT val >= 15 FROM nums ORDER BY id"},
		{"GreaterOrEqual_20", "SELECT val >= 20 FROM nums ORDER BY id"},
		{"GreaterOrEqual_10", "SELECT val >= 10 FROM nums ORDER BY id"},
		{"GreaterOrEqual_0", "SELECT val >= 0 FROM nums ORDER BY id"},
		{"GreaterOrEqual_Neg5", "SELECT val >= -5 FROM nums ORDER BY id"},
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
		{"LessThan_25", "SELECT val < 25 FROM nums ORDER BY id"},
		{"LessThan_20", "SELECT val < 20 FROM nums ORDER BY id"},
		{"LessThan_10", "SELECT val < 10 FROM nums ORDER BY id"},
		{"LessThan_0", "SELECT val < 0 FROM nums ORDER BY id"},
		{"LessThan_Neg5", "SELECT val < -5 FROM nums ORDER BY id"},
		{"LessOrEqual_25", "SELECT val <= 25 FROM nums ORDER BY id"},
		{"LessOrEqual_20", "SELECT val <= 20 FROM nums ORDER BY id"},
		{"LessOrEqual_10", "SELECT val <= 10 FROM nums ORDER BY id"},
		{"LessOrEqual_0", "SELECT val <= 0 FROM nums ORDER BY id"},
		{"LessOrEqual_Neg5", "SELECT val <= -5 FROM nums ORDER BY id"},
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
		{"Where_Equal", "SELECT * FROM nums WHERE val = 20 ORDER BY id"},
		{"Where_NotEqual", "SELECT * FROM nums WHERE val <> 20 ORDER BY id"},
		{"Where_Greater", "SELECT * FROM nums WHERE val > 15 ORDER BY id"},
		{"Where_GreaterOrEqual", "SELECT * FROM nums WHERE val >= 20 ORDER BY id"},
		{"Where_Less", "SELECT * FROM nums WHERE val < 30 ORDER BY id"},
		{"Where_LessOrEqual", "SELECT * FROM nums WHERE val <= 20 ORDER BY id"},
		{"Where_Combine_AND", "SELECT * FROM nums WHERE val >= 15 AND val <= 25 ORDER BY id"},
		{"Where_Combine_OR", "SELECT * FROM nums WHERE val = 10 OR val = 30 ORDER BY id"},
		{"Where_Combine_NOT", "SELECT * FROM nums WHERE NOT (val = 20) ORDER BY id"},
		{"Where_Multiple_AND", "SELECT * FROM nums WHERE val > 5 AND val < 25 ORDER BY id"},
		{"Where_Multiple_OR", "SELECT * FROM nums WHERE val = 10 OR val = 20 OR val = 30 ORDER BY id"},
		{"Where_NOT_AND", "SELECT * FROM nums WHERE NOT (val >= 15 AND val <= 25) ORDER BY id"},
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
		{"OrderBy_ASC", "SELECT val FROM nums ORDER BY val ASC"},
		{"OrderBy_DESC", "SELECT val FROM nums ORDER BY val DESC"},
		{"OrderBy_GreaterThan", "SELECT val FROM nums WHERE val > 5 ORDER BY val"},
		{"OrderBy_LessThan", "SELECT val FROM nums WHERE val < 30 ORDER BY val DESC"},
		{"OrderBy_Expression", "SELECT val FROM nums ORDER BY val * -1"},
		{"OrderBy_ABS", "SELECT val FROM nums ORDER BY ABS(val)"},
		{"OrderBy_WithNulls", "SELECT val FROM nums ORDER BY val ASC NULLS FIRST"},
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
		{"NULL_Equal", "SELECT val = 10 FROM nums ORDER BY id"},
		{"NULL_NotEqual", "SELECT val <> 10 FROM nums ORDER BY id"},
		{"NULL_Greater", "SELECT val > 10 FROM nums ORDER BY id"},
		{"NULL_GreaterOrEqual", "SELECT val >= 10 FROM nums ORDER BY id"},
		{"NULL_Less", "SELECT val < 10 FROM nums ORDER BY id"},
		{"NULL_LessOrEqual", "SELECT val <= 10 FROM nums ORDER BY id"},
		{"NULL_IS_NULL", "SELECT val IS NULL FROM nums ORDER BY id"},
		{"NULL_IS_NOT_NULL", "SELECT val IS NOT NULL FROM nums ORDER BY id"},
		{"NULL_IS_NULL_True", "SELECT * FROM nums WHERE val IS NULL"},
		{"NULL_IS_NOT_NULL_True", "SELECT * FROM nums WHERE val IS NOT NULL"},
	}

	for _, tt := range nullComparisonTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE reals (id INTEGER PRIMARY KEY, val REAL)")
	sqliteDB.Exec("CREATE TABLE reals (id INTEGER PRIMARY KEY, val REAL)")

	sqlvibeDB.Exec("INSERT INTO reals VALUES (1, 10.5), (2, 20.5), (3, 30.5), (4, NULL)")
	sqliteDB.Exec("INSERT INTO reals VALUES (1, 10.5), (2, 20.5), (3, 30.5), (4, NULL)")

	realComparisonTests := []struct {
		name string
		sql  string
	}{
		{"Real_Equal", "SELECT val = 20.5 FROM reals ORDER BY id"},
		{"Real_NotEqual", "SELECT val <> 20.5 FROM reals ORDER BY id"},
		{"Real_Greater", "SELECT val > 20.0 FROM reals ORDER BY id"},
		{"Real_Less", "SELECT val < 25.0 FROM reals ORDER BY id"},
		{"Real_GreaterOrEqual", "SELECT val >= 20.5 FROM reals ORDER BY id"},
		{"Real_LessOrEqual", "SELECT val <= 20.5 FROM reals ORDER BY id"},
	}

	for _, tt := range realComparisonTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE mixed (id INTEGER PRIMARY KEY, i_val INTEGER, r_val REAL)")
	sqliteDB.Exec("CREATE TABLE mixed (id INTEGER PRIMARY KEY, i_val INTEGER, r_val REAL)")

	sqlvibeDB.Exec("INSERT INTO mixed VALUES (1, 10, 10.5), (2, 20, 20.5), (3, 30, 30.5)")
	sqliteDB.Exec("INSERT INTO mixed VALUES (1, 10, 10.5), (2, 20, 20.5), (3, 30, 30.5)")

	mixedTypeTests := []struct {
		name string
		sql  string
	}{
		{"Mixed_Integer_Real_Equal", "SELECT i_val = r_val FROM mixed ORDER BY id"},
		{"Mixed_Integer_Real_Greater", "SELECT i_val > 15.0 FROM mixed ORDER BY id"},
		{"Mixed_Integer_Real_Less", "SELECT r_val < 25 FROM mixed ORDER BY id"},
		{"Mixed_Compare_Columns", "SELECT i_val < r_val FROM mixed ORDER BY id"},
		{"Mixed_Compare_With_Constant", "SELECT i_val > 15 AND r_val < 30.0 FROM mixed ORDER BY id"},
	}

	for _, tt := range mixedTypeTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	expressionComparisonTests := []struct {
		name string
		sql  string
	}{
		{"Expr_Equal", "SELECT 5 + 5 = 10 FROM nums WHERE id = 1"},
		{"Expr_Greater", "SELECT val * 2 > 20 FROM nums ORDER BY id"},
		{"Expr_Less", "SELECT val - 5 < 10 FROM nums ORDER BY id"},
		{"Expr_Combine", "SELECT (val + 10) / 2 >= 15 FROM nums ORDER BY id"},
		{"Expr_With_NULL", "SELECT val + 10 > 15 FROM nums ORDER BY id"},
	}

	for _, tt := range expressionComparisonTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	inBetweenTests := []struct {
		name string
		sql  string
	}{
		{"IN_List", "SELECT val IN (10, 20, 30) FROM nums ORDER BY id"},
		{"NOT_IN_List", "SELECT val NOT IN (10, 20) FROM nums ORDER BY id"},
		{"IN_Empty", "SELECT val IN () FROM nums WHERE id = 1"},
		{"BETWEEN_True", "SELECT val BETWEEN 10 AND 20 FROM nums ORDER BY id"},
		{"NOT_BETWEEN_True", "SELECT val NOT BETWEEN 10 AND 20 FROM nums ORDER BY id"},
		{"BETWEEN_Negative", "SELECT val BETWEEN -10 AND 10 FROM nums ORDER BY id"},
	}

	for _, tt := range inBetweenTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
