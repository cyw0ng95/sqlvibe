package E041

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E04108_L1(t *testing.T) {
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
		{"CreateIndexSingle", "CREATE INDEX idx_t1_a ON t1(a)"},
		{"CreateIndexSingleDesc", "CREATE INDEX idx_t1_b ON t1(b DESC)"},
		{"CreateIndexUnique", "CREATE UNIQUE INDEX idx_t1_c ON t1(c)"},
		{"CreateIndexComposite", "CREATE INDEX idx_t1_ab ON t1(a, b)"},
		{"CreateIndexCompositeDesc", "CREATE INDEX idx_t1_ab_desc ON t1(a DESC, b DESC)"},
		{"CreateTable2", "CREATE TABLE t2 (x INTEGER, y INTEGER, z TEXT)"},
		{"CreateIndexThreeCols", "CREATE INDEX idx_t2_xyz ON t2(x, y, z)"},
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
		{"InsertIntoT1", "INSERT INTO t1 VALUES (1, 'test1', 3.14), (2, 'test2', 6.28), (3, 'test3', 9.42)"},
		{"InsertIntoT2", "INSERT INTO t2 VALUES (1, 2, 'a'), (3, 4, 'b'), (5, 6, 'c')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	createIfNotExistsTests := []struct {
		name string
		sql  string
	}{
		{"CreateIndexIfExists", "CREATE INDEX idx_t1_a ON t1(a)"},
		{"CreateIndexIfNotExists", "CREATE INDEX IF NOT EXISTS idx_t1_a ON t1(a)"},
		{"CreateNewIndexIfNotExists", "CREATE INDEX IF NOT EXISTS idx_t1_ac ON t1(a, c)"},
	}

	for _, tt := range createIfNotExistsTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectFromT1", "SELECT * FROM t1 ORDER BY a"},
		{"SelectFromT1UsingIndex", "SELECT a, b FROM t1 WHERE a > 1"},
		{"SelectFromT2", "SELECT * FROM t2 ORDER BY x, y"},
		{"CheckIndexesExist", "SELECT name FROM sqlite_master WHERE type='index' ORDER BY name"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
