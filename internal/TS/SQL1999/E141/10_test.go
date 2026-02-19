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
		{"NullWithOr", "SELECT * FROM test_nulls WHERE val = 10 OR val = NULL"},
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
		{"InsertData", "INSERT INTO items VALUES (1, 'Item1', 10.00, 5, NULL), (2, 'Item2', 20.00, 3, 0.50), (3, 'Item3', 30.00, 2, 0.50), (4, 'Item4', NULL, NULL, NULL), (5, 'Item5', 15.00, 10, NULL), (6, 'Item6', 25.00, NULL, 0.25), (7, 'Item7', NULL, NULL, 0.75), (8, 'Item8', 35.00, 1.00, NULL)"},
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
		{"NullWithNull", "SELECT id, price * NULL FROM items WHERE id = 1"},
		{"NullAddition", "SELECT id, price + discount FROM items WHERE id = 1"},
		{"NullSubtraction", "SELECT id, price - discount FROM items WHERE id = 2"},
		{"NullDivision", "SELECT id, price / quantity FROM items WHERE id = 1"},
		{"NullModulo", "SELECT id, quantity % 3 FROM items WHERE id = 1"},
		{"NullInFunction", "SELECT id, ROUND(discount, 2) FROM items WHERE id = 1"},
		{"NullInCase", "SELECT id, CASE WHEN discount IS NULL THEN price ELSE price - discount END AS final_price FROM items WHERE id = 1"},
		{"NullWithAnd", "SELECT * FROM items WHERE discount IS NULL AND quantity IS NULL"},
		{"NullWithOr", "SELECT * FROM items WHERE price IS NULL OR discount IS NULL"},
		{"NullInSubquery", "SELECT * FROM items WHERE id IN (SELECT id FROM items WHERE discount IS NULL)"},
		{"NullMultipleNulls", "SELECT * FROM items WHERE price IS NULL AND quantity IS NULL"},
		{"NullWithFunction", "SELECT id, COUNT(*) FROM items WHERE discount IS NULL"},
		{"NullNotInWhere", "SELECT * FROM items WHERE discount IS NOT NULL"},
		{"NullWithExpression", "SELECT id, price + discount FROM items WHERE price * quantity > 100.00"},
		{"NullComplexExpression", "SELECT id, (price + discount) * 0.1 AS discounted FROM items WHERE discount IS NULL"},
		{"NullInSelectList", "SELECT id, name, price, quantity, discount FROM items"},
		{"NullInCoalesce", "SELECT id, COALESCE(discount, 0) * quantity AS total FROM items WHERE discount IS NULL"},
		{"NullInNullif", "SELECT id, NULLIF(discount, 0) * quantity FROM items WHERE discount IS NULL"},
		{"NullInCast", "SELECT id, CAST(price AS INTEGER) FROM items WHERE id = 1"},
			{"NullComplex", "SELECT * FROM items WHERE price IS NULL AND quantity IS NULL AND discount IS NULL"},
		{"NullOneNotNull", "SELECT * FROM items WHERE price IS NOT NULL OR quantity IS NOT NULL OR discount IS NOT NULL"},
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
		{"InsertData", "INSERT INTO strings VALUES (1, 'hello', 'world'), (2, 'test', 'data'), (3, NULL, NULL), (4, 'example', 'text'), (5, NULL, NULL), (6, 'sample', NULL)"},
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
		{"NullConcatenation", "SELECT id, val1 || val2 AS concat FROM strings"},
		{"NullConcatLeft", "SELECT id, val1 || NULL FROM strings WHERE id = 2"},
		{"NullConcatRight", "SELECT id, NULL || val2 FROM strings WHERE id = 2"},
		{"NullConcatBoth", "SELECT id, NULL || NULL FROM strings WHERE id = 1"},
		{"NullInLength", "SELECT id, LENGTH(val1) FROM strings WHERE id = 2"},
		{"NullLengthNull", "SELECT id, LENGTH(val1) FROM strings WHERE id = 2"},
		{"NullInUpper", "SELECT id, UPPER(val1) FROM strings WHERE id = 2"},
		{"NullUpperNull", "SELECT id, UPPER(val1) FROM strings WHERE id = 2"},
		{"NullInLower", "SELECT id, LOWER(val1) FROM strings WHERE id = 2"},
		{"NullLowerNull", "SELECT id, LOWER(val1) FROM strings WHERE id = 2"},
		{"NullInSubstring", "SELECT id, SUBSTR(val1, 1, 3) FROM strings WHERE id = 2"},
		{"NullSubstringNull", "SELECT id, SUBSTR(val1, 1, 3) FROM strings WHERE id = 2"},
		{"NullInTrim", "SELECT id, TRIM(val1) FROM strings WHERE id = 2"},
		{"NullTrimNull", "SELECT id, TRIM(val1) FROM strings WHERE id = 2"},
		{"NullInReplace", "SELECT id, REPLACE(val1, 'l', 'L') FROM strings WHERE id = 2"},
		{"NullReplaceNull", "SELECT id, REPLACE(val1, 'l', 'L') FROM strings WHERE id = 2"},
		{"NullInCoalesce", "SELECT id, COALESCE(val1, val2) FROM strings WHERE id = 2"},
		{"NullInCase", "SELECT id, CASE WHEN val1 IS NULL THEN 'Empty' ELSE val1 END FROM strings WHERE id = 2"},
		{"NullInWhere", "SELECT * FROM strings WHERE val2 IS NULL"},
		{"NullNotInWhere", "SELECT * FROM strings WHERE val2 IS NOT NULL"},
		{"NullInOrderBy", "SELECT * FROM strings WHERE val2 IS NULL ORDER BY val2"},
		{"NullInGroupBy", "SELECT val2, COUNT(*) FROM strings GROUP BY val2"},
		{"NullInHaving", "SELECT val2, COUNT(*) FROM strings GROUP BY val2 HAVING COUNT(*) > 1"},
		{"NullLike", "SELECT * FROM strings WHERE val1 LIKE 'h%'"},
		{"NullNotLike", "SELECT * FROM strings WHERE val1 NOT LIKE 't%'},
		{"NullInBetween", "SELECT * FROM strings WHERE LENGTH(val1) BETWEEN 3 AND 10"},
		{"NullIn", "SELECT * FROM strings WHERE val1 IN (SELECT id FROM strings WHERE id <= 3)"},
		{"NullNotIn", "SELECT * FROM strings WHERE val1 NOT IN (SELECT id FROM strings WHERE id <= 3)"},
		{"NullWithAnd", "SELECT * FROM strings WHERE val1 IS NOT NULL AND val2 IS NULL"},
		{"NullWithOr", "SELECT * FROM strings WHERE val1 IS NULL OR val2 IS NULL"},
		{"NullInSubquery", "SELECT * FROM strings WHERE id IN (SELECT id FROM strings WHERE val1 IS NULL)"},
		{"NullNotInSubquery", "SELECT * FROM strings WHERE id NOT IN (SELECT id FROM strings WHERE val1 IS NULL)"},
		{"NullInDistinct", "SELECT DISTINCT val1 FROM strings WHERE id <= 3"},
		{"NullWithExpression", "SELECT id, LENGTH(val1) + LENGTH(val2) FROM strings"},
		{"NullConcatMultiple", "SELECT id, val1 || '-' || COALESCE(val2, 'empty') FROM strings"},
		{"NullInSubquery", "SELECT * FROM strings WHERE id IN (SELECT id FROM strings WHERE val1 IS NULL)"},
		{"NullWithSubqueryInWhere", "SELECT * FROM strings WHERE val1 IS NULL AND id > 2"},
			{"NullWithSubqueryInOrderBy", "SELECT * FROM strings WHERE val1 IS NULL ORDER BY val2 IS NULL LAST"},
		{"NullWithSubqueryInGroupBy", "SELECT val2, COUNT(*) FROM strings GROUP BY val2 WHERE val1 IS NULL"},
		{"NullInHaving", "SELECT val2, COUNT(*) FROM strings GROUP BY val2 HAVING COUNT(*) > 1"},
		{"NullInCase", "SELECT id, CASE WHEN val1 IS NULL THEN 'Empty' ELSE val1 END FROM strings"},
		}

	for _, tt := range nullInStringsTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
