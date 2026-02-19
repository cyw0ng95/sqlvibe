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
		{"InsertData", "INSERT INTO test_nulls VALUES (1, 10, 'hello', 1.5), (2, 20, 'world', 2.5), (3, NULL, NULL, NULL), (4, NULL, 'test', 3.5), (5, 30, 'data', NULL), (6, 40, NULL, 4.0), (7, 50, 'more', NULL), (8, NULL, NULL, NULL)"},
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
		{"NullWithOr", "SELECT * FROM test_nulls WHERE val = NULL OR val = 20"},
		{"NullInSubquery", "SELECT * FROM test_nulls WHERE id IN (SELECT id FROM test_nulls WHERE val IS NULL)"},
		{"NullNotInSubquery", "SELECT * FROM test_nulls WHERE id NOT IN (SELECT id FROM test_nulls WHERE val IS NULL)"},
		{"NullInWhere", "SELECT * FROM test_nulls WHERE val IS NULL AND id > 2"},
		{"NullInOrderBy", "SELECT * FROM test_nulls WHERE val IS NOT NULL ORDER BY val"},
		{"NullInGroupBy", "SELECT text_val, COUNT(*) FROM test_nulls WHERE val IS NULL GROUP BY text_val"},
		{"NullInHaving", "SELECT val, COUNT(*) FROM test_nulls GROUP BY val HAVING COUNT(*) > 1"},
		{"NullInCase", "SELECT id, CASE WHEN val IS NULL THEN 'Null' ELSE 'NotNull' END FROM test_nulls"},
	}

	for _, tt := range nullInComparisonTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E14110_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE items (id INTEGER, name TEXT, price REAL, quantity INTEGER, discount REAL)"},
		{"InsertData", "INSERT INTO items VALUES (1, 'Item1', 10.00, 5, NULL), (2, 'Item2', 20.00, 3, NULL), (3, 'Item3', 30.00, 2, 0.50), (4, 'Item4', NULL, NULL, NULL), (5, 'Item5', 15.00, 10, NULL), (6, 'Item6', 25.00, NULL, 0.25), (7, 'Item7', NULL, NULL, 0.75), (8, 'Item8', 35.00, 1, NULL)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	nullInExpressionsTests := []struct {
		name string
		sql  string
	}{
		{"NullArithmetic", "SELECT id, price * quantity AS total FROM items"},
		{"NullWithNull", "SELECT id, price * NULL FROM items"},
		{"NullAddition", "SELECT id, price + discount FROM items"},
		{"NullSubtraction", "SELECT id, price - discount FROM items"},
		{"NullDivision", "SELECT id, price / quantity FROM items"},
		{"NullModulo", "SELECT id, quantity % 3 FROM items"},
		{"NullInFunction", "SELECT id, ROUND(discount, 2) FROM items"},
		{"NullInCase", "SELECT id, CASE WHEN discount IS NULL THEN price ELSE price - discount END AS final_price FROM items"},
		{"NullInWhere", "SELECT * FROM items WHERE discount IS NULL"},
		{"NullNotInWhere", "SELECT * FROM items WHERE discount IS NOT NULL"},
		{"NullInOrderBy", "SELECT * FROM items ORDER BY discount IS NULL LAST"},
		{"NullInLimit", "SELECT * FROM items WHERE discount IS NULL LIMIT 3"},
		{"NullInGroupBy", "SELECT discount, COUNT(*) FROM items GROUP BY discount"},
		{"NullInHaving", "SELECT discount, COUNT(*) FROM items GROUP BY discount HAVING COUNT(*) > 2"},
		{"NullWithExpression", "SELECT id, (price * quantity) + discount AS total FROM items"},
		{"NullInSubquery", "SELECT * FROM items WHERE id IN (SELECT id FROM items WHERE discount IS NULL)"},
		{"NullMultipleNulls", "SELECT * FROM items WHERE price IS NULL AND quantity IS NULL"},
		{"NullOneNotNull", "SELECT * FROM items WHERE discount IS NULL OR name IS NOT NULL"},
		{"NullWithAnd", "SELECT * FROM items WHERE price IS NULL AND quantity IS NULL"},
		{"NullWithOr", "SELECT * FROM items WHERE price IS NULL OR discount IS NOT NULL"},
		{"NullInSelectList", "SELECT id, name, price, quantity, discount FROM items"},
		{"NullWithCoalesce", "SELECT id, COALESCE(discount, 0) * quantity AS total FROM items"},
		{"NullWithNullif", "SELECT id, NULLIF(discount, NULL) * quantity FROM items"},
		{"NullInCast", "SELECT id, CAST(price AS INTEGER) FROM items"},
	}

	for _, tt := range nullInExpressionsTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E14111_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE strings (id INTEGER, val1 TEXT, val2 TEXT)"},
		{"InsertData", "INSERT INTO strings VALUES (1, 'hello', 'world'), (2, 'test', NULL), (3, NULL, 'data'), (4, 'example', 'text'), (5, NULL, NULL), (6, 'sample', NULL)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	nullInStringsTests := []struct {
		name string
		sql  string
	}{
		{"NullConcatenation", "SELECT id, val1 || val2 FROM strings"},
		{"NullConcatLeft", "SELECT id, val1 || NULL FROM strings"},
		{"NullConcatRight", "SELECT id, NULL || val2 FROM strings"},
		{"NullConcatBoth", "SELECT id, NULL || NULL FROM strings"},
		{"NullInLength", "SELECT id, LENGTH(val1) FROM strings"},
		{"NullLengthNull", "SELECT id, LENGTH(val1) FROM strings WHERE id = 2"},
		{"NullInUpper", "SELECT id, UPPER(val1) FROM strings"},
		{"NullUpperNull", "SELECT id, UPPER(val1) FROM strings WHERE id = 2"},
		{"NullInLower", "SELECT id, LOWER(val1) FROM strings"},
		{"NullLowerNull", "SELECT id, LOWER(val1) FROM strings WHERE id = 2"},
		{"NullInSubstring", "SELECT id, SUBSTR(val1, 1, 3) FROM strings"},
		{"NullSubstringNull", "SELECT id, SUBSTR(val1, 1, 3) FROM strings WHERE id = 2"},
		{"NullInTrim", "SELECT id, TRIM(val1) FROM strings"},
		{"NullTrimNull", "SELECT id, TRIM(val1) FROM strings WHERE id = 2"},
		{"NullInReplace", "SELECT id, REPLACE(val1, 'l', 'L') FROM strings"},
		{"NullInCoalesce", "SELECT id, COALESCE(val1, val2) FROM strings"},
		{"NullInCase", "SELECT id, CASE WHEN val1 IS NULL THEN 'Empty' ELSE val1 END FROM strings"},
		{"NullInWhere", "SELECT * FROM strings WHERE val2 IS NULL"},
		{"NullInOrderBy", "SELECT * FROM strings ORDER BY val1 IS NULL LAST"},
		{"NullInGroupBy", "SELECT val2, COUNT(*) FROM strings GROUP BY val2"},
		{"NullInHaving", "SELECT val1, COUNT(*) FROM strings GROUP BY val1 HAVING COUNT(*) > 1"},
		{"NullLike", "SELECT * FROM strings WHERE val1 LIKE 'h%'"},
		{"NullNotLike", "SELECT * FROM strings WHERE val1 NOT LIKE 't%'},
		{"NullInBetween", "SELECT * FROM strings WHERE LENGTH(val1) BETWEEN 3 AND 10"},
		{"NullInIn", "SELECT * FROM strings WHERE val1 IN ('hello', 'example', 'sample')"},
		{"NullNotIn", "SELECT * FROM strings WHERE val1 NOT IN ('test', 'data')"},
		{"NullWithAnd", "SELECT * FROM strings WHERE val1 IS NOT NULL AND val2 IS NULL"},
		{"NullWithOr", "SELECT * FROM strings WHERE val1 IS NULL OR val2 IS NULL"},
		{"NullInSubquery", "SELECT * FROM strings WHERE id IN (SELECT id FROM strings WHERE val1 IS NULL)"},
		{"NullInDistinct", "SELECT DISTINCT val1 FROM strings WHERE id <= 3"},
		{"NullWithExpression", "SELECT id, LENGTH(val1 || val2) FROM strings"},
		{"NullConcatMultiple", "SELECT id, val1 || '-' || COALESCE(val2, 'empty') FROM strings"},
	}

	for _, tt := range nullInStringsTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
