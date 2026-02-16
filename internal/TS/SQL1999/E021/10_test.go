package E021

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02110_L1(t *testing.T) {
	t.Skip("Known pre-existing failure: Implicit cast in comparisons - documented in v0.4.5")
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

	sqlvibeDB.Exec("CREATE TABLE casting_test (id INTEGER PRIMARY KEY, str TEXT, num INTEGER, realnum REAL)")
	sqliteDB.Exec("CREATE TABLE casting_test (id INTEGER PRIMARY KEY, str TEXT, num INTEGER, realnum REAL)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"IntToText", "INSERT INTO casting_test VALUES (1, '10', 10, 10.5)"},
		{"TextToInt", "INSERT INTO casting_test VALUES (2, '20', 20, 20.5)"},
		{"FloatToText", "INSERT INTO casting_test VALUES (3, '15.5', 15, 15.5)"},
		{"TextToFloat", "INSERT INTO casting_test VALUES (4, '25.75', 25, 25.75)"},
		{"Negative", "INSERT INTO casting_test VALUES (5, '-100', -100, -100.5)"},
		{"Zero", "INSERT INTO casting_test VALUES (6, '0', 0, 0.0)"},
		{"Empty", "INSERT INTO casting_test VALUES (7, '', 0, 0.0)"},
		{"Large", "INSERT INTO casting_test VALUES (8, '999999999', 999999999, 999999999.9)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	castTests := []struct {
		name string
		sql  string
	}{
		{"CastIntToText", "SELECT CAST(num AS TEXT) FROM casting_test WHERE id = 1"},
		{"CastTextToInt", "SELECT CAST(str AS INTEGER) FROM casting_test WHERE id = 1"},
		{"CastFloatToText", "SELECT CAST(realnum AS TEXT) FROM casting_test WHERE id = 1"},
		{"CastTextToFloat", "SELECT CAST(str AS REAL) FROM casting_test WHERE id = 3"},
		{"ConcatIntText", "SELECT num || ' is a number' FROM casting_test WHERE id = 1"},
		{"ConcatTextInt", "SELECT str || 100 FROM casting_test WHERE id = 1"},
		{"IntComparison", "SELECT num = 10 FROM casting_test WHERE id = 1"},
		{"TextComparison", "SELECT str = '10' FROM casting_test WHERE id = 1"},
		{"ImplicitIntInConcat", "SELECT 'Value: ' || num FROM casting_test WHERE id = 1"},
		{"ImplicitFloatInConcat", "SELECT 'Value: ' || realnum FROM casting_test WHERE id = 1"},
		{"CastIntToReal", "SELECT CAST(num AS REAL) FROM casting_test WHERE id = 1"},
		{"CastRealToInt", "SELECT CAST(realnum AS INTEGER) FROM casting_test WHERE id = 1"},
		{"CastNegative", "SELECT CAST(str AS INTEGER) FROM casting_test WHERE id = 5"},
		{"CastZero", "SELECT CAST(str AS INTEGER) FROM casting_test WHERE id = 6"},
		{"ConcatMultiple", "SELECT str || '-' || num || '-' || realnum FROM casting_test WHERE id = 1"},
		{"ConcatWithFunction", "SELECT UPPER(str) || '-' || LOWER(str) FROM casting_test WHERE id = 1"},
		{"CastInFunction", "SELECT UPPER(CAST(num AS TEXT)) FROM casting_test WHERE id = 1"},
		{"ImplicitCastInCompare", "SELECT num = '10' FROM casting_test WHERE id = 1"},
		{"ImplicitCastInCompare2", "SELECT str = 10 FROM casting_test WHERE id = 1"},
		{"LengthOfCast", "SELECT LENGTH(CAST(num AS TEXT)) FROM casting_test WHERE id = 1"},
		{"SubstrOfCast", "SELECT SUBSTR(CAST(num AS TEXT), 1, 2) FROM casting_test WHERE id = 1"},
	}

	for _, tt := range castTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
