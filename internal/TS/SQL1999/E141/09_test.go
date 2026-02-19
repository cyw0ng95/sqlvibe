package E141

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E14109_L1(t *testing.T) {
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
			{"CreateTable", "CREATE TABLE test_nulls (id INTEGER, val INTEGER, text_val TEXT, real_val REAL)"},
		{"InsertData", "INSERT INTO test_nulls VALUES (1, 10, hello, 1.5), (2, 20, world, 2.5), (3, 30, data, NULL), (4, NULL, test, 3.5), (5, 30, data, NULL), (6, 40, NULL, 4.0), (7, 50, more, NULL), (8, NULL, NULL, NULL)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	nullInComparisonTests := []struct {
		name string
		sql  string
	}{
			{"NullEquals", "SELECT * FROM test_nulls WHERE val = NULL"},
			{"NullNotEquals", "SELECT * FROM test_nulls WHERE val <> NULL"},
			{"NullLessThan", "SELECT * FROM test_nulls WHERE val < NULL"},
			{"NullLessThanOrEqual", "SELECT * FROM test_nulls WHERE val <= NULL"},
			{"NullGreaterThan", "SELECT * FROM test_nulls WHERE val > NULL"},
			{"NullGreaterThanOrEqual", "SELECT * FROM test_nulls WHERE val >= NULL"},
			{"NullInExpression", "SELECT * FROM test_nulls WHERE val + 10 = NULL"},
			{"NullMultiple", "SELECT * FROM test_nulls WHERE val * 2 = NULL"},
			{"NullDivision", "SELECT * FROM test_nulls WHERE val / 2 = NULL"},
			{"NullModulo", "SELECT * FROM test_nulls WHERE val % 3 = NULL"},
			{"NullWithAnd", "SELECT * FROM test_nulls WHERE val = 10 AND val = NULL"},
			{"NullWithOr", "SELECT * FROM test_nulls WHERE val = 10 OR val = NULL"},
			{"NullInSubquery", "SELECT * FROM test_nulls WHERE id IN (SELECT id FROM test_nulls WHERE val IS NULL)"},
			{"NullNotInSubquery", "SELECT * FROM test_nulls WHERE id NOT IN (SELECT id FROM test_nulls WHERE val IS NULL)"},
			{"NullInWhere", "SELECT * FROM test_nulls WHERE val IS NULL AND id > 2"},
			{"NullInOrderBy", "SELECT * FROM test_nulls WHERE val IS NOT NULL ORDER BY val"},
			{"NullInGroupBy", "SELECT text_val, COUNT(*) FROM test_nulls WHERE val IS NULL GROUP BY text_val"},
			{"NullInHaving", "SELECT val, COUNT(*) FROM test_nulls GROUP BY val HAVING COUNT(*) > 1"},
			{"NullInCase", "SELECT id, CASE WHEN val IS NULL THEN 0 ELSE 1 END FROM test_nulls"},
		}

	for _, tt := range nullInComparisonTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

