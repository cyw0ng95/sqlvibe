package E041

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E04107_L1(t *testing.T) {
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
		{"CreateTable1", "CREATE TABLE t1 (a INTEGER, b TEXT)"},
		{"CreateTable2", "CREATE TABLE t2 (x INTEGER, y REAL)"},
		{"CreateTable3", "CREATE TABLE t3 (id INTEGER PRIMARY KEY, name TEXT)"},
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
		{"InsertIntoT1", "INSERT INTO t1 VALUES (1, 'test1'), (2, 'test2')"},
		{"InsertIntoT2", "INSERT INTO t2 VALUES (10, 3.14), (20, 6.28)"},
		{"InsertIntoT3", "INSERT INTO t3 VALUES (1, 'Alice'), (2, 'Bob')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	dropTests := []struct {
		name string
		sql  string
	}{
		{"DropTable1", "DROP TABLE t1"},
		{"DropTable2", "DROP TABLE t2"},
		{"DropTable3", "DROP TABLE t3"},
	}

	for _, tt := range dropTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	createTests2 := []struct {
		name string
		sql  string
	}{
		{"CreateTable4", "CREATE TABLE t4 (a INTEGER, b TEXT)"},
		{"CreateTable5", "CREATE TABLE t5 (x INTEGER, y REAL)"},
	}

	for _, tt := range createTests2 {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	dropIfExistsTests := []struct {
		name string
		sql  string
	}{
		{"DropIfExistsExisting", "DROP TABLE IF EXISTS t4"},
		{"DropIfExistsNonExisting", "DROP TABLE IF EXISTS non_existent_table"},
	}

	for _, tt := range dropIfExistsTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectFromT5", "SELECT * FROM t5"},
		{"CheckT4Exists", "SELECT name FROM sqlite_master WHERE type='table' AND name='t4'"},
		{"CheckT5Exists", "SELECT name FROM sqlite_master WHERE type='table' AND name='t5'"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
