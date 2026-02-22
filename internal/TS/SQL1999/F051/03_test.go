package F051

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F503_F05103_L1(t *testing.T) {
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
		{"CreateVarchar10", "CREATE TABLE t1 (a VARCHAR(10))"},
		{"CreateVarchar50", "CREATE TABLE t2 (a VARCHAR(50))"},
		{"CreateVarchar255", "CREATE TABLE t3 (a VARCHAR(255))"},
		{"CreateVarchar1000", "CREATE TABLE t4 (a VARCHAR(1000))"},
		{"CreateText", "CREATE TABLE t5 (a TEXT)"},
		{"CreateMultipleVarchar", "CREATE TABLE t6 (a VARCHAR(10), b VARCHAR(50), c TEXT)"},
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
		{"InsertEmpty", "INSERT INTO t1 VALUES ('')"},
		{"InsertShort", "INSERT INTO t1 VALUES ('a')"},
		{"InsertLong", "INSERT INTO t1 VALUES ('hello world')"},
		{"InsertT2", "INSERT INTO t2 VALUES ('short')"},
		{"InsertT2_2", "INSERT INTO t2 VALUES ('this is a longer string')"},
		{"InsertT3", "INSERT INTO t3 VALUES ('a longer text field with many characters')"},
		{"InsertT4", "INSERT INTO t4 VALUES ('very long text that might exceed typical column sizes')"},
		{"InsertT5", "INSERT INTO t5 VALUES ('any length text')"},
		{"InsertT5_2", "INSERT INTO t5 VALUES ('Lorem ipsum dolor sit amet, consectetur adipiscing elit.')"},
		{"InsertT6", "INSERT INTO t6 VALUES ('a', 'bbb', 'cccc')"},
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
		{"SelectT4", "SELECT * FROM t4"},
		{"SelectT5", "SELECT * FROM t5"},
		{"SelectT6", "SELECT * FROM t6"},
		{"SelectLength", "SELECT LENGTH(a) FROM t1"},
		{"SelectLike", "SELECT * FROM t2 WHERE a LIKE '%hello%'"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
