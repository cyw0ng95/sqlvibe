package E041

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E04102_L1(t *testing.T) {
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
		{"NotNull", "CREATE TABLE t1 (a INTEGER NOT NULL)"},
		{"MultipleNotNull", "CREATE TABLE t2 (a INTEGER NOT NULL, b TEXT NOT NULL)"},
		{"Unique", "CREATE TABLE t3 (a INTEGER UNIQUE)"},
		{"MultipleUnique", "CREATE TABLE t4 (a INTEGER UNIQUE, b TEXT UNIQUE)"},
		{"Default", "CREATE TABLE t5 (a INTEGER DEFAULT 0)"},
		{"TextDefault", "CREATE TABLE t6 (a TEXT DEFAULT 'default')"},
		{"MixedConstraints", "CREATE TABLE t7 (a INTEGER NOT NULL DEFAULT 0, b TEXT UNIQUE)"},
		{"RealDefault", "CREATE TABLE t8 (a REAL DEFAULT 3.14)"},
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
		{"InsertValidNotNull", "INSERT INTO t1 VALUES (1)"},
		{"InsertWithDefault", "INSERT INTO t5 VALUES ()"},
		{"InsertWithTextDefault", "INSERT INTO t6 VALUES ()"},
		{"InsertWithRealDefault", "INSERT INTO t8 VALUES ()"},
		{"InsertUniqueValue", "INSERT INTO t3 VALUES (1)"},
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
		{"SelectFromT5", "SELECT * FROM t5"},
		{"SelectFromT6", "SELECT * FROM t6"},
		{"SelectFromT8", "SELECT * FROM t8"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
