package E041

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E04106_L1(t *testing.T) {
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
		{"CreateBaseTable", "CREATE TABLE t1 (a INTEGER, b TEXT)"},
		{"AddColumnInt", "ALTER TABLE t1 ADD COLUMN c INTEGER"},
		{"AddColumnText", "ALTER TABLE t1 ADD COLUMN d TEXT"},
		{"AddColumnWithDefault", "ALTER TABLE t1 ADD COLUMN e INTEGER DEFAULT 0"},
		{"AddColumnWithNotNull", "ALTER TABLE t1 ADD COLUMN f TEXT NOT NULL DEFAULT 'default'"},
		{"RenameTable", "CREATE TABLE t2 (x INTEGER, y TEXT)"},
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
		{"InsertIntoT1", "INSERT INTO t1 (a, b) VALUES (1, 'test')"},
		{"InsertWithNewColumns", "INSERT INTO t1 (a, b, c, d) VALUES (2, 'test2', 3, 'test3')"},
		{"InsertWithDefaults", "INSERT INTO t1 (a, b) VALUES (3, 'test4')"},
		{"InsertIntoT2", "INSERT INTO t2 VALUES (1, 'test')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	alterTests := []struct {
		name string
		sql  string
	}{
		{"RenameT2ToNewName", "ALTER TABLE t2 RENAME TO t2_renamed"},
		{"RenameT1ToNewName", "ALTER TABLE t1 RENAME TO t1_renamed"},
	}

	for _, tt := range alterTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectFromT1Renamed", "SELECT * FROM t1_renamed"},
		{"SelectFromT2Renamed", "SELECT * FROM t2_renamed"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
