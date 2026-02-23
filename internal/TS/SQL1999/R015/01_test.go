package R015

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_R015_R01501_L1(t *testing.T) {
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
	sqlvibeDB.Exec("CREATE INDEX idx_name ON t1(name)")
	sqliteDB.Exec("CREATE INDEX idx_name ON t1(name)")
	sqlvibeDB.Exec("CREATE INDEX idx_score ON t1(score)")
	sqliteDB.Exec("CREATE INDEX idx_score ON t1(score)")

	dropTests := []struct {
		name string
		sql  string
	}{
		{"DropIndex", "DROP INDEX idx_name"},
		{"DropIndexIfExists", "DROP INDEX IF EXISTS idx_score"},
		{"DropIndexIfExistsNonExistent", "DROP INDEX IF EXISTS idx_nonexistent"},
	}
	for _, tt := range dropTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
