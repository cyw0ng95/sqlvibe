package E041

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E04101_L1(t *testing.T) {
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
		{"SimpleIntTable", "CREATE TABLE t1 (a INTEGER)"},
		{"SimpleTextTable", "CREATE TABLE t2 (a TEXT)"},
		{"SimpleRealTable", "CREATE TABLE t3 (a REAL)"},
		{"SimpleBLOBTable", "CREATE TABLE t4 (a BLOB)"},
		{"MultipleColsSameType", "CREATE TABLE t5 (a INTEGER, b INTEGER, c INTEGER)"},
		{"MixedTypes", "CREATE TABLE t6 (a INTEGER, b TEXT, c REAL, d BLOB)"},
		{"EmptyTable", "CREATE TABLE t7 ()"},
		{"TableNameWithUnderscore", "CREATE TABLE test_table (id INTEGER)"},
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
		{"InsertIntoIntTable", "INSERT INTO t1 VALUES (42)"},
		{"InsertIntoTextTable", "INSERT INTO t2 VALUES ('hello')"},
		{"InsertIntoRealTable", "INSERT INTO t3 VALUES (3.14)"},
		{"InsertMultipleValues", "INSERT INTO t5 VALUES (1, 2, 3)"},
		{"InsertIntoMixed", "INSERT INTO t6 VALUES (1, 'test', 2.5, x'0102')"},
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
		{"SelectFromT1", "SELECT * FROM t1"},
		{"SelectFromT2", "SELECT * FROM t2"},
		{"SelectFromT5", "SELECT * FROM t5"},
		{"SelectFromT6", "SELECT * FROM t6"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
