package F041

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F402_F04102_L1(t *testing.T) {
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
		{"CreateIntTable", "CREATE TABLE t1 (id INTEGER, val INTEGER)"},
		{"CreateTextTable", "CREATE TABLE t2 (id INTEGER, val TEXT)"},
		{"CreateRealTable", "CREATE TABLE t3 (id INTEGER, val REAL)"},
		{"CreateMixedTable", "CREATE TABLE t4 (id INTEGER, name TEXT, price REAL, data BLOB)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	insertTests := []struct {
		name string
		sql  string
	}{
		{"InsertIntPos", "INSERT INTO t1 VALUES (1, 100)"},
		{"InsertIntNeg", "INSERT INTO t1 VALUES (2, -50)"},
		{"InsertIntZero", "INSERT INTO t1 VALUES (3, 0)"},
		{"InsertText", "INSERT INTO t2 VALUES (1, 'hello')"},
		{"InsertTextEmpty", "INSERT INTO t2 VALUES (2, '')"},
		{"InsertTextSpecial", "INSERT INTO t2 VALUES (3, 'hello''world')"},
		{"InsertReal", "INSERT INTO t3 VALUES (1, 3.14)"},
		{"InsertRealNeg", "INSERT INTO t3 VALUES (2, -2.718)"},
		{"InsertRealZero", "INSERT INTO t3 VALUES (3, 0.0)"},
		{"InsertMixed", "INSERT INTO t4 VALUES (1, 'test', 9.99, X'0102')"},
		{"InsertMultiple", "INSERT INTO t1 VALUES (4, 200), (5, 300), (6, 400)"},
		{"InsertIntoText", "INSERT INTO t2 (id) VALUES (4)"},
		{"InsertIntoInt", "INSERT INTO t1 (id) VALUES (7)"},
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
		{"SelectAllT1", "SELECT * FROM t1 ORDER BY id"},
		{"SelectAllT2", "SELECT * FROM t2 ORDER BY id"},
		{"SelectAllT3", "SELECT * FROM t3 ORDER BY id"},
		{"SelectAllT4", "SELECT * FROM t4"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
