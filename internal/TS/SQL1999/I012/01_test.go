package I012

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_I012_I01201_L1(t *testing.T) {
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

	createTests := []struct {
		name string
		sql  string
	}{
		{"CreateWithCheck", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, age INTEGER CHECK (age >= 0), name TEXT)"},
		{"CreateWithMultiCheck", "CREATE TABLE t2 (id INTEGER PRIMARY KEY, score INTEGER CHECK (score >= 0 AND score <= 100))"},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Valid inserts - should succeed
	validTests := []struct {
		name string
		sql  string
	}{
		{"InsertValidAge", "INSERT INTO t1 VALUES (1, 25, 'Alice')"},
		{"InsertZeroAge", "INSERT INTO t1 VALUES (2, 0, 'Baby')"},
		{"InsertValidScore", "INSERT INTO t2 VALUES (1, 75)"},
		{"InsertMinScore", "INSERT INTO t2 VALUES (2, 0)"},
		{"InsertMaxScore", "INSERT INTO t2 VALUES (3, 100)"},
	}
	for _, tt := range validTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAll", "SELECT * FROM t1 ORDER BY id"},
		{"SelectScores", "SELECT * FROM t2 ORDER BY id"},
		{"SelectWhereCheck", "SELECT * FROM t1 WHERE age > 0"},
		{"SelectSumScores", "SELECT sum(score) FROM t2"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
