package F081

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F806_F08106_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, val TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, val TEXT)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')"},
		{"InsertT2", "INSERT INTO t2 VALUES (3, 'c'), (4, 'd'), (5, 'e')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	exceptTests := []struct {
		name string
		sql  string
	}{
		{"ExceptBasic", "SELECT val FROM t1 EXCEPT SELECT val FROM t2"},
		{"ExceptAll", "SELECT val FROM t1 EXCEPT ALL SELECT val FROM t2"},
		{"ExceptSelf", "SELECT val FROM t1 EXCEPT SELECT val FROM t1 WHERE val = 'a'"},
		{"ExceptEmpty", "SELECT val FROM t1 EXCEPT SELECT val FROM t2 WHERE val = 'nonexistent'"},
	}

	for _, tt := range exceptTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F807_F08107_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, val TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, val TEXT)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')"},
		{"InsertT2", "INSERT INTO t2 VALUES (2, 'b'), (3, 'c'), (4, 'd')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	intersectTests := []struct {
		name string
		sql  string
	}{
		{"IntersectBasic", "SELECT val FROM t1 INTERSECT SELECT val FROM t2"},
		{"IntersectAll", "SELECT val FROM t1 INTERSECT ALL SELECT val FROM t2"},
		{"IntersectSelf", "SELECT val FROM t1 INTERSECT SELECT val FROM t1"},
		{"IntersectEmpty", "SELECT val FROM t1 INTERSECT SELECT val FROM t2 WHERE val = 'nonexistent'"},
	}

	for _, tt := range intersectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F808_F08108_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, val TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, val TEXT)"},
		{"CreateT3", "CREATE TABLE t3 (id INTEGER, val TEXT)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'a'), (2, 'b')"},
		{"InsertT2", "INSERT INTO t2 VALUES (3, 'c'), (4, 'd')"},
		{"InsertT3", "INSERT INTO t3 VALUES (5, 'e'), (6, 'f')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	multipleUnionTests := []struct {
		name string
		sql  string
	}{
		{"MultipleUnion", "SELECT val FROM t1 UNION SELECT val FROM t2 UNION SELECT val FROM t3"},
		{"MultipleUnionAll", "SELECT val FROM t1 UNION ALL SELECT val FROM t2 UNION ALL SELECT val FROM t3"},
		{"UnionMixed", "SELECT val FROM t1 UNION ALL SELECT val FROM t2 UNION SELECT val FROM t3"},
		{"UnionWithOrderBy", "SELECT val FROM t1 UNION SELECT val FROM t2 UNION SELECT val FROM t3 ORDER BY val"},
	}

	for _, tt := range multipleUnionTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F809_F08109_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, val TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, val TEXT)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 'a'), (2, 'b'), (3, 'c')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	unionLimitTests := []struct {
		name string
		sql  string
	}{
		{"UnionLimit", "SELECT val FROM t1 UNION SELECT val FROM t2 LIMIT 3"},
		{"UnionLimitOffset", "SELECT val FROM t1 UNION SELECT val FROM t2 LIMIT 2 OFFSET 2"},
		{"UnionAllLimit", "SELECT val FROM t1 UNION ALL SELECT val FROM t2 LIMIT 4"},
		{"UnionOrderByLimit", "SELECT val FROM t1 UNION SELECT val FROM t2 ORDER BY val LIMIT 3"},
	}

	for _, tt := range unionLimitTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F810_F08110_L1(t *testing.T) {
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

	setupTests := []struct {
		name string
		sql  string
	}{
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, val TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, val TEXT)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')"},
		{"InsertT2", "INSERT INTO t2 VALUES (4, 'd'), (5, 'e'), (6, 'f')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	unionWithOrderByTests := []struct {
		name string
		sql  string
	}{
		{"UnionAllOrderBy", "SELECT val FROM t1 UNION ALL SELECT val FROM t2 ORDER BY val"},
		{"UnionOrderByDesc", "SELECT val FROM t1 UNION SELECT val FROM t2 ORDER BY val DESC"},
		{"UnionOrderByNumeric", "SELECT id, val FROM t1 UNION SELECT id, val FROM t2 ORDER BY id"},
		{"UnionOrderByExpression", "SELECT val FROM t1 UNION SELECT val FROM t2 ORDER BY LENGTH(val)"},
	}

	for _, tt := range unionWithOrderByTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
