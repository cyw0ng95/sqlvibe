package F041

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F406_F04106_L1(t *testing.T) {
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
		{"CreateWithPK", "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)"},
		{"CreateWithUnique", "CREATE TABLE t2 (id INTEGER UNIQUE, val TEXT)"},
		{"CreateWithNotNull", "CREATE TABLE t3 (id INTEGER NOT NULL, val TEXT)"},
		{"CreateWithDefault", "CREATE TABLE t4 (id INTEGER, val TEXT DEFAULT 'default')"},
		{"CreateWithCheck", "CREATE TABLE t5 (id INTEGER CHECK(id > 0), val TEXT)"},
		{"CreateWithPKAuto", "CREATE TABLE t6 (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)"},
		{"CreateWithMultiple", "CREATE TABLE t7 (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER DEFAULT 0)"},
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
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'one')"},
		{"InsertT1_2", "INSERT INTO t1 VALUES (2, 'two')"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 'one')"},
		{"InsertT2_2", "INSERT INTO t2 VALUES (2, 'two')"},
		{"InsertT3", "INSERT INTO t3 VALUES (1, 'one')"},
		{"InsertT3_2", "INSERT INTO t3 (val) VALUES ('two')"},
		{"InsertT4", "INSERT INTO t4 (id) VALUES (1)"},
		{"InsertT4_2", "INSERT INTO t4 VALUES (2, 'explicit')"},
		{"InsertT5", "INSERT INTO t5 VALUES (1, 'one')"},
		{"InsertT6", "INSERT INTO t6 (val) VALUES ('auto')"},
		{"InsertT6_2", "INSERT INTO t6 (val) VALUES ('auto2')"},
		{"InsertT7", "INSERT INTO t7 VALUES (1, 'Alice', 30)"},
		{"InsertT7_2", "INSERT INTO t7 (id, name) VALUES (2, 'Bob')"},
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
		{"SelectT6", "SELECT * FROM t6"},
		{"SelectT7", "SELECT * FROM t7 ORDER BY id"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
