package L011

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_L011_L01101_L1(t *testing.T) {
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
		{"CreateWithReservedWords", `CREATE TABLE t1 ("select" INTEGER, "from" TEXT, "where" INTEGER)`},
		{"CreateWithMoreReserved", `CREATE TABLE t2 ("order" INTEGER, "group" TEXT, "having" INTEGER)`},
	}
	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec(`INSERT INTO t1 VALUES (1, 'test', 42)`)
	sqliteDB.Exec(`INSERT INTO t1 VALUES (1, 'test', 42)`)
	sqlvibeDB.Exec(`INSERT INTO t1 VALUES (2, 'hello', 99)`)
	sqliteDB.Exec(`INSERT INTO t1 VALUES (2, 'hello', 99)`)

	sqlvibeDB.Exec(`INSERT INTO t2 VALUES (10, 'grp1', 5)`)
	sqliteDB.Exec(`INSERT INTO t2 VALUES (10, 'grp1', 5)`)

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectAllT1", "SELECT * FROM t1 ORDER BY 1"},
		{"SelectAllT2", "SELECT * FROM t2"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
