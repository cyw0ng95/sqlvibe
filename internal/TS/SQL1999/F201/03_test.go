package F201

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F202_F26103_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b NUMERIC, c TEXT)"},
		{"InsertValues", "INSERT INTO t1 VALUES (100, 123.45, 'hello')"},
		{"InsertNull", "INSERT INTO t1 VALUES (NULL, NULL, NULL)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	numericCastTests := []struct {
		name string
		sql  string
	}{
		{"CastToNumeric", "SELECT CAST(a AS NUMERIC) FROM t1 WHERE a = 100"},
		{"CastToDecimal", "SELECT CAST(b AS DECIMAL(10,2)) FROM t1 WHERE a = 100"},
		{"CastTextToNumeric", "SELECT CAST('123.45' AS NUMERIC)"},
		{"CastIntegerToNumeric", "SELECT CAST(100 AS NUMERIC)"},
		{"CastNumericPrecision", "SELECT CAST(123.456 AS DECIMAL(5,2))"},
	}

	for _, tt := range numericCastTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F203_F26104_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (a TEXT)"},
		{"InsertValues", "INSERT INTO t1 VALUES ('hello'), (X'48656C6C6F'), (NULL)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	blobCastTests := []struct {
		name string
		sql  string
	}{
		{"CastToBlob", "SELECT CAST('test' AS BLOB)"},
		{"CastFromBlob", "SELECT CAST(X'48656C6C6F' AS TEXT)"},
		{"CastInInsert", "INSERT INTO t1 SELECT CAST('inserted' AS BLOB)"},
		{"CastBlobToText", "SELECT CAST(a AS TEXT) FROM t1 WHERE a IS NOT NULL"},
	}

	for _, tt := range blobCastTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F204_F26105_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b TEXT, c REAL)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'hello', 3.14), (2, 'world', 2.71)"},
		{"InsertNull", "INSERT INTO t1 VALUES (NULL, NULL, NULL)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	castInDmlTests := []struct {
		name string
		sql  string
	}{
		{"CastInInsert", "INSERT INTO t1 (a) SELECT CAST('10' AS INTEGER)"},
		{"CastInSelect", "SELECT CAST(a AS TEXT) AS a_text FROM t1 WHERE a = 1"},
		{"CastInUpdate", "UPDATE t1 SET b = CAST(a AS TEXT) WHERE a = 1"},
		{"CastExpression", "SELECT CAST(a + 10 AS TEXT) FROM t1 WHERE a = 1"},
		{"CastChained", "SELECT CAST(CAST(a AS TEXT) AS INTEGER) FROM t1 WHERE a = 1"},
	}

	for _, tt := range castInDmlTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
