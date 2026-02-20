package E051

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E05102_L1(t *testing.T) {
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
		{"TEXT", "CREATE TABLE t1 (a TEXT)"},
		{"VARCHAR", "CREATE TABLE t2 (a VARCHAR)"},
		{"VARCHARWithLength", "CREATE TABLE t3 (a VARCHAR(10))"},
		{"VARCHARWithLength255", "CREATE TABLE t4 (a VARCHAR(255))"},
		{"CHAR", "CREATE TABLE t5 (a CHAR)"},
		{"CHARWithLength", "CREATE TABLE t6 (a CHAR(10))"},
		{"CHARACTER", "CREATE TABLE t7 (a CHARACTER)"},
		{"CHARACTERWithLength", "CREATE TABLE t8 (a CHARACTER(10))"},
		{"NCHAR", "CREATE TABLE t9 (a NCHAR)"},
		{"NVARCHAR", "CREATE TABLE t10 (a NVARCHAR)"},
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
		{"InsertText", "INSERT INTO t1 VALUES ('hello')"},
		{"InsertEmptyText", "INSERT INTO t2 VALUES ('')"},
		{"InsertVarchar", "INSERT INTO t3 VALUES ('test')"},
		{"InsertVarcharLong", "INSERT INTO t4 VALUES ('longer text')"},
		{"InsertChar", "INSERT INTO t5 VALUES ('a')"},
		{"InsertCharWithLength", "INSERT INTO t6 VALUES ('test')"},
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
		{"SelectT6", "SELECT * FROM t6"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
