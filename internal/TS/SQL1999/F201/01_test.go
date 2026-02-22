package F201

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F201_F26101_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (i INTEGER, r REAL, t TEXT, n NUMERIC)"},
		{"InsertData", "INSERT INTO t1 VALUES (42, 3.14, 'hello', 123.45)"},
		{"InsertData2", "INSERT INTO t1 VALUES (100, 2.718, 'world', 999.99)"},
		{"InsertNull", "INSERT INTO t1 VALUES (NULL, NULL, NULL, NULL)"},
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
		{"CastIntToText", "SELECT CAST(i AS TEXT) FROM t1 WHERE i = 42"},
		{"CastIntToReal", "SELECT CAST(i AS REAL) FROM t1 WHERE i = 42"},
		{"CastIntToInteger", "SELECT CAST(i AS INTEGER) FROM t1 WHERE i = 42"},
		{"CastRealToText", "SELECT CAST(r AS TEXT) FROM t1 WHERE i = 42"},
		{"CastRealToInteger", "SELECT CAST(r AS INTEGER) FROM t1 WHERE i = 42"},
		{"CastTextToInteger", "SELECT CAST(t AS INTEGER) FROM t1 WHERE t = 'hello'"},
		{"CastTextToReal", "SELECT CAST(t AS REAL) FROM t1 WHERE t = 'hello'"},
		{"CastNumericToInteger", "SELECT CAST(n AS INTEGER) FROM t1 WHERE i = 42"},
		{"CastNumericToText", "SELECT CAST(n AS TEXT) FROM t1 WHERE i = 42"},
		{"CastNumericToReal", "SELECT CAST(n AS REAL) FROM t1 WHERE i = 42"},
		{"CastNullToText", "SELECT CAST(i AS TEXT) FROM t1 WHERE i IS NULL"},
		{"CastNullToInteger", "SELECT CAST(i AS INTEGER) FROM t1 WHERE i IS NULL"},
		{"CastChain", "SELECT CAST(CAST(i AS TEXT) AS INTEGER) FROM t1 WHERE i = 42"},
		{"CastInExpression", "SELECT CAST(i AS REAL) + 10 FROM t1 WHERE i = 42"},
		{"CastInWhere", "SELECT * FROM t1 WHERE CAST(i AS REAL) > 50"},
	}

	for _, tt := range castTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F201_F26102_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t2 (a TEXT, b INTEGER)"},
		{"Insert1", "INSERT INTO t2 VALUES ('10', 10)"},
		{"Insert2", "INSERT INTO t2 VALUES ('20', 20)"},
		{"Insert3", "INSERT INTO t2 VALUES ('abc', 30)"},
		{"Insert4", "INSERT INTO t2 VALUES ('30.5', 40)"},
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
		{"CastTextToIntValid", "SELECT CAST(a AS INTEGER) FROM t2 WHERE a = '10'"},
		{"CastTextToIntInvalid", "SELECT CAST(a AS INTEGER) FROM t2 WHERE a = 'abc'"},
		{"CastTextToRealValid", "SELECT CAST(a AS REAL) FROM t2 WHERE a = '10'"},
		{"CastTextToRealDecimal", "SELECT CAST(a AS REAL) FROM t2 WHERE a = '30.5'"},
		{"CastIntToText", "SELECT CAST(b AS TEXT) FROM t2 WHERE b = 10"},
		{"CastInSelectList", "SELECT b, CAST(b AS TEXT) as b_text FROM t2 ORDER BY b"},
		{"CastInGroupBy", "SELECT CAST(b/10 AS INTEGER), COUNT(*) FROM t2 GROUP BY CAST(b/10 AS INTEGER)"},
		{"CastInOrderBy", "SELECT * FROM t2 ORDER BY CAST(b AS INTEGER) DESC"},
	}

	for _, tt := range castTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
