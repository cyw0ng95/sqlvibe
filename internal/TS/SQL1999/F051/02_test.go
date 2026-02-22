package F051

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F502_F05102_L1(t *testing.T) {
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
		{"CreateChar1", "CREATE TABLE t1 (a CHAR(1))"},
		{"CreateChar5", "CREATE TABLE t2 (a CHAR(5))"},
		{"CreateChar10", "CREATE TABLE t3 (a CHAR(10))"},
		{"CreateCharFixed", "CREATE TABLE t4 (a CHAR)"},
		{"CreateMultipleChar", "CREATE TABLE t5 (a CHAR(5), b CHAR(10))"},
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
		{"InsertT1", "INSERT INTO t1 VALUES ('a')"},
		{"InsertT1_2", "INSERT INTO t1 VALUES ('z')"},
		{"InsertT2", "INSERT INTO t2 VALUES ('hello')"},
		{"InsertT2_2", "INSERT INTO t2 VALUES ('hi')"},
		{"InsertT3", "INSERT INTO t3 VALUES ('test')"},
		{"InsertT3_2", "INSERT INTO t3 VALUES ('longer text')"},
		{"InsertT4", "INSERT INTO t4 VALUES ('x')"},
		{"InsertT5", "INSERT INTO t5 VALUES ('hi', 'there')"},
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
		{"SelectConcat", "SELECT a || b FROM t5"},
		{"SelectUpper", "SELECT UPPER(a) FROM t2"},
		{"SelectLower", "SELECT LOWER(a) FROM t2"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
