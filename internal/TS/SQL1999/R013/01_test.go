package R013

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_R013_R01301_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)")
	sqlvibeDB.Exec("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val REAL)")
	sqliteDB.Exec("CREATE TABLE t2 (id INTEGER PRIMARY KEY, val REAL)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 'Alice')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 'Alice')")

	dropTests := []struct {
		name string
		sql  string
	}{
		{"DropTable", "DROP TABLE t1"},
		{"DropTableIfExists", "DROP TABLE IF EXISTS t2"},
		{"DropTableIfExistsNonExistent", "DROP TABLE IF EXISTS t_nonexistent"},
	}
	for _, tt := range dropTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_R013_R01302_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t_keep (id INTEGER PRIMARY KEY, name TEXT)")
	sqliteDB.Exec("CREATE TABLE t_keep (id INTEGER PRIMARY KEY, name TEXT)")
	sqlvibeDB.Exec("CREATE TABLE t_drop (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE t_drop (id INTEGER PRIMARY KEY, val TEXT)")
	sqlvibeDB.Exec("INSERT INTO t_keep VALUES (1, 'remaining')")
	sqliteDB.Exec("INSERT INTO t_keep VALUES (1, 'remaining')")

	SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, "DROP TABLE t_drop", "DropTable")

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectKeptTable", "SELECT * FROM t_keep ORDER BY id"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
