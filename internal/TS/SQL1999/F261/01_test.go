package F261

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F26101_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER, score INTEGER)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER, score INTEGER)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 95)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 95)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 85)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 85)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (3, 75)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (3, 75)")

	tests := []struct {
		name string
		sql  string
	}{
		{"CaseSimple", "SELECT CASE id WHEN 1 THEN 'One' WHEN 2 THEN 'Two' ELSE 'Other' END FROM t1"},
		{"CaseSimple2", "SELECT CASE id WHEN 1 THEN 'One' END FROM t1"},
		{"CaseSearched", "SELECT CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' ELSE 'C' END FROM t1"},
		{"CaseSearchedWithElse", "SELECT CASE WHEN score > 100 THEN 'Invalid' ELSE 'Valid' END FROM t1"},
		{"CaseWithoutElse", "SELECT CASE WHEN score >= 90 THEN 'A' END FROM t1"},
		{"CaseInWhere", "SELECT * FROM t1 WHERE CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' END = 'A'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_F26102_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (id INTEGER, val INTEGER)")
	sqliteDB.Exec("CREATE TABLE t1 (id INTEGER, val INTEGER)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 10)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, NULL)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, NULL)")

	tests := []struct {
		name string
		sql  string
	}{
		{"CaseNested", "SELECT CASE WHEN id = 1 THEN CASE WHEN val > 5 THEN 'High' ELSE 'Low' END ELSE 'Other' END FROM t1"},
		{"CaseWithAggregate", "SELECT CASE WHEN COUNT(*) > 1 THEN 'Many' ELSE 'Few' END FROM t1"},
		{"CaseInOrderBy", "SELECT * FROM t1 ORDER BY CASE WHEN id = 1 THEN 0 ELSE 1 END"},
		{"CaseWithMath", "SELECT CASE WHEN val IS NOT NULL THEN val * 2 ELSE 0 END FROM t1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_F26103_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (a INTEGER, b INTEGER)")
	sqliteDB.Exec("CREATE TABLE t1 (a INTEGER, b INTEGER)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 1)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 1)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, 2)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, 2)")

	tests := []struct {
		name string
		sql  string
	}{
		{"NullIf", "SELECT NULLIF(a, b) FROM t1"},
		{"NullIf2", "SELECT NULLIF(1, 1)"},
		{"NullIf3", "SELECT NULLIF(1, 2)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_F26104_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE t1 (a INTEGER, b TEXT)")
	sqliteDB.Exec("CREATE TABLE t1 (a INTEGER, b TEXT)")

	sqlvibeDB.Exec("INSERT INTO t1 VALUES (1, NULL)")
	sqliteDB.Exec("INSERT INTO t1 VALUES (1, NULL)")
	sqlvibeDB.Exec("INSERT INTO t1 VALUES (2, 'hello')")
	sqliteDB.Exec("INSERT INTO t1 VALUES (2, 'hello')")

	tests := []struct {
		name string
		sql  string
	}{
		{"Coalesce", "SELECT COALESCE(b, 'default') FROM t1"},
		{"Coalesce2", "SELECT COALESCE(NULL, 'a', 'b')"},
		{"Coalesce3", "SELECT COALESCE('a', 'b')"},
		{"CoalesceWithMultiple", "SELECT COALESCE(a, b, 'fallback') FROM t1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
