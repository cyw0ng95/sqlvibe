package F041

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F401_F04101_L1(t *testing.T) {
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
		{"CreateT2", "CREATE TABLE t2 (a INTEGER, b INTEGER, c INTEGER)"},
		{"CreateT3", "CREATE TABLE t3 (x TEXT, y TEXT, z TEXT)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	insertTests := []struct {
		name string
		sql  string
	}{
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'one')"},
		{"InsertT1_2", "INSERT INTO t1 VALUES (2, 'two')"},
		{"InsertT2", "INSERT INTO t2 VALUES (10, 20, 30)"},
		{"InsertT3", "INSERT INTO t3 VALUES ('a', 'b', 'c')"},
		{"InsertT3_2", "INSERT INTO t3 VALUES ('x', 'y', 'z')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	selectTests := []struct {
		name string
		sql  string
	}{
		{"SelectT1", "SELECT * FROM t1"},
		{"SelectT2", "SELECT * FROM t2"},
		{"SelectT3", "SELECT * FROM t3"},
		{"SelectT1Where", "SELECT * FROM t1 WHERE id = 1"},
		{"SelectT2Where", "SELECT * FROM t2 WHERE a > 5"},
		{"SelectT3Where", "SELECT * FROM t3 WHERE x = 'a'"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
