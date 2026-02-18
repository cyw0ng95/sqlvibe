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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b TEXT, c REAL)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'hello', 3.14), (2, NULL, 2.71), (NULL, 'world', 1.5), (4, 'test', NULL)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	nullTextTests := []struct {
		name string
		sql  string
	}{
		{"IsNullText", "SELECT * FROM t1 WHERE b IS NULL"},
		{"IsNotNullText", "SELECT * FROM t1 WHERE b IS NOT NULL"},
		{"NullWithComparison", "SELECT * FROM t1 WHERE a > 1 OR b IS NULL"},
		{"NullInFunction", "SELECT UPPER(b) FROM t1 WHERE b IS NOT NULL"},
	}

	for _, tt := range nullTextTests {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b INTEGER)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 10), (2, NULL), (3, 30), (NULL, 40)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	nullInAggregationTests := []struct {
		name string
		sql  string
	}{
		{"CountStar", "SELECT COUNT(*) FROM t1"},
		{"CountColumn", "SELECT COUNT(b) FROM t1"},
		{"SumWithNull", "SELECT SUM(b) FROM t1"},
		{"AvgWithNull", "SELECT AVG(b) FROM t1"},
		{"MinMaxWithNull", "SELECT MIN(b), MAX(b) FROM t1"},
		{"CountDistinctWithNull", "SELECT COUNT(DISTINCT b) FROM t1"},
	}

	for _, tt := range nullInAggregationTests {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, name TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, value INTEGER)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, NULL)"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 100), (2, NULL), (4, 400)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	nullInJoinTests := []struct {
		name string
		sql  string
	}{
		{"LeftJoinNull", "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id WHERE t2.id IS NULL"},
		{"InnerJoinWithNull", "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"},
		{"NullInJoinCondition", "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id OR t2.id IS NULL"},
		{"CoalesceInJoin", "SELECT t1.id, COALESCE(t2.value, 0) FROM t1 LEFT JOIN t2 ON t1.id = t2.id"},
	}

	for _, tt := range nullInJoinTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E14112_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 10, 100), (2, 20, 200), (3, NULL, 300)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	coalesceNullifTests := []struct {
		name string
		sql  string
	}{
		{"CoalesceThreeArgs", "SELECT COALESCE(a, b, c) FROM t1"},
		{"CoalesceWithDefault", "SELECT COALESCE(b, -1) FROM t1"},
		{"CoalesceInExpression", "SELECT COALESCE(a, 0) + 100 FROM t1"},
		{"NullIfEqual", "SELECT NULLIF(a, b) FROM t1"},
		{"NullIfNotEqual", "SELECT NULLIF(a, 999) FROM t1"},
		{"NullIfExpression", "SELECT NULLIF(a + b, 21) FROM t1"},
	}

	for _, tt := range coalesceNullifTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
