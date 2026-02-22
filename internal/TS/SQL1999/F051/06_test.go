package F051

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F506_F05106_L1(t *testing.T) {
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
		{"CreateNullable", "CREATE TABLE t1 (id INTEGER, val INTEGER)"},
		{"CreateNotNull", "CREATE TABLE t2 (id INTEGER, val INTEGER NOT NULL)"},
		{"CreateWithDefaultInt", "CREATE TABLE t3 (id INTEGER, val INTEGER DEFAULT 0)"},
		{"CreateWithDefaultText", "CREATE TABLE t4 (id INTEGER, val TEXT DEFAULT 'hello')"},
		{"CreateWithDefaultExpr", "CREATE TABLE t5 (id INTEGER, val INTEGER DEFAULT (1+1))"},
		{"CreateWithNullDefault", "CREATE TABLE t6 (id INTEGER, val INTEGER DEFAULT NULL)"},
		{"CreateWithMultipleDefaults", "CREATE TABLE t7 (a INTEGER DEFAULT 1, b TEXT DEFAULT 'x', c INTEGER DEFAULT 0)"},
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
		{"InsertExplicitNull", "INSERT INTO t1 VALUES (1, NULL)"},
		{"InsertT1", "INSERT INTO t1 VALUES (2, 100)"},
		{"InsertT1_2", "INSERT INTO t1 (id) VALUES (3)"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 100)"},
		{"InsertT2_2", "INSERT INTO t2 (id) VALUES (2)"},
		{"InsertT3", "INSERT INTO t3 (id) VALUES (1)"},
		{"InsertT3_2", "INSERT INTO t3 VALUES (2, 50)"},
		{"InsertT3_3", "INSERT INTO t3 (id, val) VALUES (3, NULL)"},
		{"InsertT4", "INSERT INTO t4 (id) VALUES (1)"},
		{"InsertT4_2", "INSERT INTO t4 VALUES (2, 'custom')"},
		{"InsertT5", "INSERT INTO t5 (id) VALUES (1)"},
		{"InsertT5_2", "INSERT INTO t5 VALUES (2, 10)"},
		{"InsertT6", "INSERT INTO t6 (id) VALUES (1)"},
		{"InsertT7", "INSERT INTO t7 (id) VALUES (1)"},
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
		{"SelectT1", "SELECT * FROM t1 ORDER BY id"},
		{"SelectT2", "SELECT * FROM t2 ORDER BY id"},
		{"SelectT3", "SELECT * FROM t3 ORDER BY id"},
		{"SelectT4", "SELECT * FROM t4 ORDER BY id"},
		{"SelectT5", "SELECT * FROM t5 ORDER BY id"},
		{"SelectT6", "SELECT * FROM t6 ORDER BY id"},
		{"SelectT7", "SELECT * FROM t7"},
		{"SelectIsNull", "SELECT * FROM t1 WHERE val IS NULL"},
		{"SelectIsNotNull", "SELECT * FROM t1 WHERE val IS NOT NULL"},
		{"SelectCoalesce", "SELECT id, COALESCE(val, -1) FROM t1"},
		{"SelectIfNull", "SELECT id, IFNULL(val, 0) FROM t1"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
