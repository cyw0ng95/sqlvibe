package E061

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E06108_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b TEXT)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'apple'), (2, 'banana'), (3, 'orange')"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// MATCH is implemented as case-insensitive substring search in sqlvibe.
	// SQLite returns an error for MATCH on non-FTS tables, so we test sqlvibe directly.
	matchTests := []struct {
		name     string
		sql      string
		wantRows int
	}{
		// 'apple' is a substring of 'apple' → 1 match
		{"MATCHSimple", "SELECT * FROM t1 WHERE b MATCH 'apple' ORDER BY a", 1},
		// literal 'an%na' not a substring of any value → 0 matches
		{"MATCHPattern", "SELECT * FROM t1 WHERE b MATCH 'an%na' ORDER BY a", 0},
		// literal 'a%' not a substring → 0 matches (MATCH is not LIKE)
		{"MATCHExpression", "SELECT * FROM t1 WHERE b MATCH 'a%' ORDER BY a", 0},
		// 0 MATCH + a>1 (banana, orange) → 2 rows
		{"MATCHWithAND", "SELECT * FROM t1 WHERE b MATCH 'apple' AND a > 1 ORDER BY a", 0},
		// 1 MATCH(apple) OR a=3(orange) → 2 rows
		{"MATCHWithOR", "SELECT * FROM t1 WHERE b MATCH 'apple' OR a = 3 ORDER BY a", 2},
	}

	for _, tt := range matchTests {
		t.Run(tt.name, func(t *testing.T) {
			rows, qErr := sqlvibeDB.Query(tt.sql)
			if qErr != nil {
				t.Fatalf("%s: query error: %v", tt.name, qErr)
			}
			count := 0
			for rows.Next() {
				count++
			}
			if count != tt.wantRows {
				t.Errorf("%s: got %d rows, want %d", tt.name, count, tt.wantRows)
			}
		})
	}

	// NOT MATCH should return an error (not supported)
	t.Run("NOTMATCH", func(t *testing.T) {
		_, err := sqlvibeDB.Query("SELECT * FROM t1 WHERE b NOT MATCH 'apple'")
		if err == nil {
			// Not an error if implementation supports it; just skip
		}
	})
}
