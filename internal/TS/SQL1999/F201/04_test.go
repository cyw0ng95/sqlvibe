package F201

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F202_F20102_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE test_data (id INTEGER, num_val INTEGER, text_val TEXT, real_val REAL)"},
		{"InsertData", "INSERT INTO test_data VALUES (1, 123, '456', 7.89), (2, '789', 456, 10.11), (3, NULL, 'string', 3.14), (4, 123.45, 678, NULL)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	castTests := []struct {
		name string
		sql  string
	}{
		{"TextToInteger", "SELECT CAST('123' AS INTEGER) FROM test_data WHERE id = 1"},
		{"TextToReal", "SELECT CAST('7.89' AS REAL) FROM test_data WHERE id = 1"},
		{"IntegerToText", "SELECT CAST(123 AS TEXT) FROM test_data WHERE id = 1"},
		{"IntegerToReal", "SELECT CAST(123 AS REAL) FROM test_data WHERE id = 1"},
		{"RealToInteger", "SELECT CAST(7.89 AS INTEGER) FROM test_data WHERE id = 1"},
		{"RealToText", "SELECT CAST(7.89 AS TEXT) FROM test_data WHERE id = 1"},
		{"NullToInteger", "SELECT CAST(NULL AS INTEGER) FROM test_data WHERE id = 3"},
		{"NullToText", "SELECT CAST(NULL AS TEXT) FROM test_data WHERE id = 3"},
		{"NullToReal", "SELECT CAST(NULL AS REAL) FROM test_data WHERE id = 3"},
		{"CastColumn", "SELECT CAST(num_val AS TEXT) FROM test_data"},
		{"CastExpression", "SELECT CAST(num_val + 10 AS REAL) FROM test_data WHERE id = 1"},
		{"CastWithFunction", "SELECT CAST(UPPER(text_val) AS TEXT) FROM test_data WHERE id = 2"},
		{"CastInWhere", "SELECT * FROM test_data WHERE CAST(text_val AS INTEGER) = 456"},
		{"CastInOrderBy", "SELECT * FROM test_data ORDER BY CAST(real_val AS INTEGER) DESC"},
		{"CastInGroupBy", "SELECT CAST(real_val AS INTEGER), COUNT(*) FROM test_data GROUP BY CAST(real_val AS INTEGER)"},
		{"CastInHaving", "SELECT CAST(real_val AS INTEGER), COUNT(*) FROM test_data GROUP BY CAST(real_val AS INTEGER) HAVING COUNT(*) > 1"},
		{"CastFloatToInt", "SELECT CAST(3.999 AS INTEGER) FROM test_data WHERE id = 1"},
		{"CastIntToFloat", "SELECT CAST(456 AS REAL) FROM test_data WHERE id = 2"},
		{"CastStringToFloat", "SELECT CAST('123.456' AS REAL) FROM test_data WHERE id = 1"},
		{"CastFloatToString", "SELECT CAST(7.89 AS TEXT) FROM test_data WHERE id = 1"},
		{"CastNegativeNumber", "SELECT CAST('-123' AS INTEGER) FROM test_data WHERE id = 1"},
		{"CastZero", "SELECT CAST('0' AS INTEGER) FROM test_data WHERE id = 1"},
		{"CastLargeNumber", "SELECT CAST('999999' AS INTEGER) FROM test_data WHERE id = 1"},
		{"CastDecimal", "SELECT CAST('12.345' AS NUMERIC) FROM test_data WHERE id = 1"},
		{"CastBoolean", "SELECT CASE WHEN CAST('1' AS INTEGER) = 1 THEN 'true' ELSE 'false' END FROM test_data"},
	}

	for _, tt := range castTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F203_F20103_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE dates (id INTEGER, date_str TEXT, num_val INTEGER, dec_val DECIMAL(10,2))"},
		{"InsertData", "INSERT INTO dates VALUES (1, '2024-01-15', 100, 123.45), (2, '2024-02-20', 200, 678.90), (3, 'not-a-date', 300, NULL)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	typeCastTests := []struct {
		name string
		sql  string
	}{
		{"StringToInt", "SELECT id, CAST(date_str AS INTEGER) FROM dates"},
		{"StringToDecimal", "SELECT id, CAST('123.45' AS DECIMAL(10,2)) FROM dates WHERE id = 1"},
		{"IntToDecimal", "SELECT id, CAST(num_val AS DECIMAL(10,2)) FROM dates"},
		{"DecimalToInt", "SELECT id, CAST(dec_val AS INTEGER) FROM dates WHERE id = 1"},
		{"DecimalToString", "SELECT id, CAST(dec_val AS TEXT) FROM dates WHERE id = 1"},
		{"StringToReal", "SELECT id, CAST('123.456' AS REAL) FROM dates WHERE id = 1"},
		{"RealToString", "SELECT id, CAST(num_val AS REAL) FROM dates"},
		{"CastInCalculation", "SELECT id, num_val * CAST('10' AS INTEGER) FROM dates"},
		{"CastWithExpression", "SELECT id, CAST(num_val + 50 AS TEXT) FROM dates"},
		{"CastNullHandling", "SELECT id, CAST(NULL AS INTEGER) FROM dates WHERE id = 1"},
		{"CastInComparison", "SELECT * FROM dates WHERE CAST(date_str AS INTEGER) = 2024"},
		{"CastInJoin", "SELECT d1.id, d2.id FROM dates d1 JOIN dates d2 ON CAST(d1.num_val AS TEXT) = CAST(d2.num_val AS TEXT)"},
		{"CastInSubquery", "SELECT * FROM dates WHERE num_val IN (SELECT CAST(num_val AS INTEGER) FROM dates WHERE id < 3)"},
		{"CastPrecision", "SELECT id, CAST(dec_val AS DECIMAL(5,1)) FROM dates WHERE id = 1"},
		{"CastRound", "SELECT id, ROUND(CAST(dec_val AS INTEGER) / 10.0, 1) FROM dates WHERE id = 1"},
		{"CastConcatenation", "SELECT id, 'Value: ' || CAST(num_val AS TEXT) FROM dates"},
		{"CastInCase", "SELECT id, CASE WHEN CAST(date_str AS INTEGER) > 2000 THEN 'Recent' ELSE 'Old' END FROM dates"},
		{"CastInAggregates", "SELECT CAST(AVG(num_val) AS INTEGER) FROM dates"},
		{"CastMinMax", "SELECT CAST(MIN(num_val) AS REAL), CAST(MAX(num_val) AS REAL) FROM dates"},
		{"CastInDistinct", "SELECT DISTINCT CAST(num_val AS TEXT) FROM dates"},
		{"CastWithLimit", "SELECT CAST(num_val AS TEXT) FROM dates LIMIT 2"},
		{"CastInOrderByExpression", "SELECT * FROM dates ORDER BY CAST(num_val AS TEXT) DESC"},
		{"CastTypeConversion", "SELECT typeof(CAST(num_val AS REAL)) FROM dates"},
		{"CastBooleanExpression", "SELECT id, CAST(num_val > 150 AS INTEGER) FROM dates"},
		{"CastMixedTypes", "SELECT CAST(CAST(num_val AS TEXT) AS INTEGER) FROM dates"},
		{"CastDateLike", "SELECT * FROM dates WHERE CAST(date_str AS TEXT) LIKE '2024-%'"},
		{"CastNumericExpression", "SELECT id, CAST(num_val * 1.5 AS INTEGER) FROM dates"},
	}

	for _, tt := range typeCastTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
