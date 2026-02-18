package E051

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E05105_L1(t *testing.T) {
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
		{"BOOLEAN", "CREATE TABLE t1 (a BOOLEAN)"},
		{"BOOL", "CREATE TABLE t2 (a BOOL)"},
		{"BIT", "CREATE TABLE t3 (a BIT)"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	insertTests := []struct {
		name string
		sql  string
	}{
		{"InsertTrue", "INSERT INTO t1 VALUES (1)"},
		{"InsertFalse", "INSERT INTO t1 VALUES (0)"},
		{"InsertBoolTrue", "INSERT INTO t2 VALUES (TRUE)"},
		{"InsertBoolFalse", "INSERT INTO t2 VALUES (FALSE)"},
		{"InsertBit1", "INSERT INTO t3 VALUES (1)"},
		{"InsertBit0", "INSERT INTO t3 VALUES (0)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectT1", "SELECT * FROM t1"},
		{"SelectT2", "SELECT * FROM t2"},
		{"SelectT3", "SELECT * FROM t3"},
		{"SelectWhereTrue", "SELECT * FROM t1 WHERE a = 1"},
		{"SelectWhereFalse", "SELECT * FROM t2 WHERE a = 0"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
