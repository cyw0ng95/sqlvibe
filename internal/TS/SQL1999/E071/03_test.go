package E071

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E07103_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE a (val INTEGER)")
	sqliteDB.Exec("CREATE TABLE a (val INTEGER)")
	sqlvibeDB.Exec("CREATE TABLE b (val INTEGER)")
	sqliteDB.Exec("CREATE TABLE b (val INTEGER)")

	sqlvibeDB.Exec("INSERT INTO a VALUES (1)")
	sqliteDB.Exec("INSERT INTO a VALUES (1)")
	sqlvibeDB.Exec("INSERT INTO a VALUES (2)")
	sqliteDB.Exec("INSERT INTO a VALUES (2)")
	sqlvibeDB.Exec("INSERT INTO a VALUES (3)")
	sqliteDB.Exec("INSERT INTO a VALUES (3)")
	sqlvibeDB.Exec("INSERT INTO b VALUES (2)")
	sqliteDB.Exec("INSERT INTO b VALUES (2)")
	sqlvibeDB.Exec("INSERT INTO b VALUES (3)")
	sqliteDB.Exec("INSERT INTO b VALUES (3)")
	sqlvibeDB.Exec("INSERT INTO b VALUES (4)")
	sqliteDB.Exec("INSERT INTO b VALUES (4)")

	tests := []struct {
		name string
		sql  string
	}{
		{"UnionAll", "SELECT val FROM a UNION ALL SELECT val FROM b ORDER BY val"},
		{"Union", "SELECT val FROM a UNION SELECT val FROM b ORDER BY val"},
		{"Intersect", "SELECT val FROM a INTERSECT SELECT val FROM b ORDER BY val"},
		{"Except", "SELECT val FROM a EXCEPT SELECT val FROM b ORDER BY val"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
