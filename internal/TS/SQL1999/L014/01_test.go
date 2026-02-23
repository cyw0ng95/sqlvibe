package L014

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_L014_L01401_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER, val INTEGER, name TEXT)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER, val INTEGER, name TEXT)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 10, 'Alice')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 10, 'Alice')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 20, 'Bob')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 20, 'Bob')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, 30, 'Carol')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, 30, 'Carol')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (4, 10, 'Dave')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (4, 10, 'Dave')")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (5, NULL, NULL)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (5, NULL, NULL)")

	queryTests := []struct {
		name string
		sql  string
	}{
		// Comparison operators
		{"CompareEq", "SELECT * FROM t1 WHERE val = 10 ORDER BY id"},
		{"CompareNeq", "SELECT * FROM t1 WHERE val != 10 ORDER BY id"},
		{"CompareLt", "SELECT * FROM t1 WHERE val < 20 ORDER BY id"},
		{"CompareGt", "SELECT * FROM t1 WHERE val > 10 ORDER BY id"},
		{"CompareLte", "SELECT * FROM t1 WHERE val <= 20 ORDER BY id"},
		{"CompareGte", "SELECT * FROM t1 WHERE val >= 20 ORDER BY id"},
		// LIKE
		{"LikePrefix", "SELECT * FROM t1 WHERE name LIKE 'A%' ORDER BY id"},
		{"LikeSuffix", "SELECT * FROM t1 WHERE name LIKE '%l' ORDER BY id"},
		{"LikeMiddle", "SELECT * FROM t1 WHERE name LIKE '%o%' ORDER BY id"},
		{"LikeSingle", "SELECT * FROM t1 WHERE name LIKE '_ob' ORDER BY id"},
		// IN
		{"InList", "SELECT * FROM t1 WHERE val IN (10, 30) ORDER BY id"},
		{"NotInList", "SELECT * FROM t1 WHERE val NOT IN (10, 20) ORDER BY id"},
		// BETWEEN
		{"Between", "SELECT * FROM t1 WHERE val BETWEEN 10 AND 20 ORDER BY id"},
		{"NotBetween", "SELECT * FROM t1 WHERE val NOT BETWEEN 10 AND 20 ORDER BY id"},
		// IS NULL / IS NOT NULL
		{"IsNull", "SELECT * FROM t1 WHERE val IS NULL"},
		{"IsNotNull", "SELECT * FROM t1 WHERE val IS NOT NULL ORDER BY id"},
		{"IsNullName", "SELECT * FROM t1 WHERE name IS NULL"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
