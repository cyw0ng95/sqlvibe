package R014

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_R014_R01401_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 'Alice', 95.5)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 'Alice', 95.5)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 'Bob', 82.0)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 'Bob', 82.0)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, 'Alice', 77.5)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, 'Alice', 77.5)")

	indexTests := []struct {
		name string
		sql  string
	}{
		{"CreateIndex", "CREATE INDEX idx_name ON t1(name)"},
		{"CreateUniqueIndex", "CREATE UNIQUE INDEX idx_id_score ON t1(id, score)"},
		{"CreateIndexIfNotExists", "CREATE INDEX IF NOT EXISTS idx_name ON t1(name)"},
	}
	for _, tt := range indexTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAfterIndex", "SELECT * FROM t1 ORDER BY id"},
		{"SelectWhereAfterIndex", "SELECT * FROM t1 WHERE name = 'Alice' ORDER BY id"},
		{"SelectScoreAfterIndex", "SELECT id, score FROM t1 ORDER BY id"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
