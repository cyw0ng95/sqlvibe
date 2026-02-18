package E041

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E04109_L1(t *testing.T) {
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
		{"CreateTable1", "CREATE TABLE t1 (a INTEGER, b TEXT, c REAL)"},
		{"CreateTable2", "CREATE TABLE t2 (x INTEGER, y INTEGER, z TEXT)"},
		{"CreateIndex1", "CREATE INDEX idx_t1_a ON t1(a)"},
		{"CreateIndex2", "CREATE INDEX idx_t1_b ON t1(b)"},
		{"CreateIndex3", "CREATE INDEX idx_t2_x ON t2(x)"},
		{"CreateUniqueIndex", "CREATE UNIQUE INDEX idx_t2_z ON t2(z)"},
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
		{"InsertIntoT1", "INSERT INTO t1 VALUES (1, 'test1', 3.14), (2, 'test2', 6.28)"},
		{"InsertIntoT2", "INSERT INTO t2 VALUES (1, 2, 'a'), (3, 4, 'b')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryBeforeDrop := []struct {
		name string
		sql  string
	}{
		{"CheckIndexesBeforeDrop", "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"},
	}

	for _, tt := range queryBeforeDrop {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	dropTests := []struct {
		name string
		sql  string
	}{
		{"DropIndex1", "DROP INDEX idx_t1_a"},
		{"DropIndex2", "DROP INDEX idx_t1_b"},
		{"DropIndex3", "DROP INDEX idx_t2_x"},
	}

	for _, tt := range dropTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	dropIfExistsTests := []struct {
		name string
		sql  string
	}{
		{"DropIfExistsExisting", "DROP INDEX IF EXISTS idx_t2_z"},
		{"DropIfExistsNonExisting", "DROP INDEX IF EXISTS non_existent_index"},
	}

	for _, tt := range dropIfExistsTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryAfterDrop := []struct {
		name string
		sql  string
	}{
		{"SelectFromT1", "SELECT * FROM t1 ORDER BY a"},
		{"SelectFromT2", "SELECT * FROM t2 ORDER BY x"},
		{"CheckIndexesAfterDrop", "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"},
	}

	for _, tt := range queryAfterDrop {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
