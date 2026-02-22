package F271

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F271_Union_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateT1", "CREATE TABLE t1 (a INTEGER, b TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (a INTEGER, b TEXT)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'one'), (2, 'two')"},
		{"InsertT2", "INSERT INTO t2 VALUES (2, 'two'), (3, 'three')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"UnionAll", "SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2"},
		{"Union", "SELECT a, b FROM t1 UNION SELECT a, b FROM t2"},
		{"UnionOrderBy", "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a"},
		{"UnionWithLimit", "SELECT a FROM t1 UNION SELECT a FROM t2 ORDER BY a LIMIT 2"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F271_Except_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateT1", "CREATE TABLE t1 (a INTEGER)"},
		{"CreateT2", "CREATE TABLE t2 (a INTEGER)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1), (2), (3)"},
		{"InsertT2", "INSERT INTO t2 VALUES (2), (3), (4)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"Except", "SELECT a FROM t1 EXCEPT SELECT a FROM t2"},
		{"ExceptOrderBy", "SELECT a FROM t1 EXCEPT SELECT a FROM t2 ORDER BY a"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F271_Intersect_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateT1", "CREATE TABLE t1 (a INTEGER)"},
		{"CreateT2", "CREATE TABLE t2 (a INTEGER)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1), (2), (3)"},
		{"InsertT2", "INSERT INTO t2 VALUES (2), (3), (4)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"Intersect", "SELECT a FROM t1 INTERSECT SELECT a FROM t2"},
		{"IntersectOrderBy", "SELECT a FROM t1 INTERSECT SELECT a FROM t2 ORDER BY a"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F271_CompoundWithAgg_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateT1", "CREATE TABLE t1 (dept TEXT, amount INTEGER)"},
		{"CreateT2", "CREATE TABLE t2 (dept TEXT, amount INTEGER)"},
		{"InsertT1", "INSERT INTO t1 VALUES ('IT', 100), ('HR', 200)"},
		{"InsertT2", "INSERT INTO t2 VALUES ('IT', 150), ('Sales', 250)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"UnionWithSum", "SELECT dept, SUM(amount) FROM (SELECT dept, amount FROM t1 UNION ALL SELECT dept, amount FROM t2) GROUP BY dept"},
		{"UnionWithCount", "SELECT dept, COUNT(*) FROM (SELECT dept, amount FROM t1 UNION ALL SELECT dept, amount FROM t2) GROUP BY dept"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
