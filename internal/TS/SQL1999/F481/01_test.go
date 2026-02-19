package F481

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F481_IsNull_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b TEXT, c REAL)"},
		{"InsertNULL", "INSERT INTO t1 VALUES (1, 'hello', 3.14)"},
		{"InsertNULL2", "INSERT INTO t1 VALUES (NULL, 'world', NULL)"},
		{"InsertNULL3", "INSERT INTO t1 VALUES (2, NULL, 2.71)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"IsNull", "SELECT * FROM t1 WHERE a IS NULL"},
		{"IsNotNull", "SELECT * FROM t1 WHERE a IS NOT NULL"},
		{"IsNullB", "SELECT * FROM t1 WHERE b IS NULL"},
		{"IsNotNullB", "SELECT * FROM t1 WHERE b IS NOT NULL"},
		{"IsNullC", "SELECT * FROM t1 WHERE c IS NULL"},
		{"IsNotNullC", "SELECT * FROM t1 WHERE c IS NOT NULL"},
		{"Combined", "SELECT * FROM t1 WHERE a IS NOT NULL AND b IS NULL"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F481_NullInExpression_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b INTEGER)"},
		{"InsertData", "INSERT INTO t1 VALUES (1, 2), (3, NULL), (NULL, 5)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"NullArithAdd", "SELECT a + b FROM t1"},
		{"NullArithSub", "SELECT a - b FROM t1"},
		{"NullArithMul", "SELECT a * b FROM t1"},
		{"NullCoalesce", "SELECT COALESCE(a, 0), COALESCE(b, 0) FROM t1"},
		{"NullIf", "SELECT NULLIF(a, 1) FROM t1"},
		{"IfNull", "SELECT IFNULL(a, 99), IFNULL(b, 99) FROM t1"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F481_NullComparison_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER)"},
		{"InsertData", "INSERT INTO t1 VALUES (1), (NULL), (3), (NULL), (5)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"Equality", "SELECT * FROM t1 WHERE a = 1"},
		{"NotEqual", "SELECT * FROM t1 WHERE a != 1"},
		{"GreaterThan", "SELECT * FROM t1 WHERE a > 1"},
		{"LessThan", "SELECT * FROM t1 WHERE a < 3"},
		{"IsNull", "SELECT * FROM t1 WHERE a IS NULL"},
		{"IsNotNull", "SELECT * FROM t1 WHERE a IS NOT NULL"},
		{"CountAll", "SELECT COUNT(*) FROM t1"},
		{"CountNotNull", "SELECT COUNT(a) FROM t1"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
