package F081

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F801_F08101_L1(t *testing.T) {
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
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, val TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, val TEXT)"},
		{"InsertT1_1", "INSERT INTO t1 VALUES (1, 'a')"},
		{"InsertT1_2", "INSERT INTO t1 VALUES (2, 'b')"},
		{"InsertT1_3", "INSERT INTO t1 VALUES (3, 'c')"},
		{"InsertT2_1", "INSERT INTO t2 VALUES (4, 'd')"},
		{"InsertT2_2", "INSERT INTO t2 VALUES (5, 'e')"},
		{"InsertT2_3", "INSERT INTO t2 VALUES (6, 'f')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	unionTests := []struct {
		name string
		sql  string
	}{
		{"UnionAllBasic", "SELECT id, val FROM t1 UNION ALL SELECT id, val FROM t2"},
		{"UnionAllSameTable", "SELECT id, val FROM t1 UNION ALL SELECT id, val FROM t1"},
	}

	for _, tt := range unionTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F802_F08102_L1(t *testing.T) {
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
		{"CreateT1", "CREATE TABLE t1 (val TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (val TEXT)"},
		{"InsertT1_1", "INSERT INTO t1 VALUES ('apple')"},
		{"InsertT1_2", "INSERT INTO t1 VALUES ('banana')"},
		{"InsertT1_3", "INSERT INTO t1 VALUES ('cherry')"},
		{"InsertT2_1", "INSERT INTO t2 VALUES ('banana')"},
		{"InsertT2_2", "INSERT INTO t2 VALUES ('date')"},
		{"InsertT2_3", "INSERT INTO t2 VALUES ('apple')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	unionTests := []struct {
		name string
		sql  string
	}{
		{"UnionDistinct", "SELECT val FROM t1 UNION SELECT val FROM t2"},
		{"UnionDistinct2", "SELECT val FROM t1 UNION SELECT val FROM t1"},
		{"UnionImplicit", "SELECT val FROM t1 UNION ALL SELECT val FROM t2 ORDER BY val"},
	}

	for _, tt := range unionTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F803_F08103_L1(t *testing.T) {
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
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, name TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, name TEXT)"},
		{"InsertT1_1", "INSERT INTO t1 VALUES (3, 'Charlie')"},
		{"InsertT1_2", "INSERT INTO t1 VALUES (1, 'Alice')"},
		{"InsertT1_3", "INSERT INTO t1 VALUES (5, 'Eve')"},
		{"InsertT2_1", "INSERT INTO t2 VALUES (4, 'Dave')"},
		{"InsertT2_2", "INSERT INTO t2 VALUES (2, 'Bob')"},
		{"InsertT2_3", "INSERT INTO t2 VALUES (6, 'Frank')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	unionTests := []struct {
		name string
		sql  string
	}{
		{"UnionOrderByName", "SELECT name FROM t1 UNION SELECT name FROM t2 ORDER BY name"},
		{"UnionOrderById", "SELECT id FROM t1 UNION SELECT id FROM t2 ORDER BY id"},
		{"UnionOrderByDesc", "SELECT name FROM t1 UNION SELECT name FROM t2 ORDER BY name DESC"},
	}

	for _, tt := range unionTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F804_F08104_L1(t *testing.T) {
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
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, name TEXT, dept TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, name TEXT, dept TEXT)"},
		{"InsertT1_1", "INSERT INTO t1 VALUES (1, 'Alice', 'Engineering')"},
		{"InsertT1_2", "INSERT INTO t1 VALUES (2, 'Bob', 'Sales')"},
		{"InsertT1_3", "INSERT INTO t1 VALUES (3, 'Charlie', 'Engineering')"},
		{"InsertT2_1", "INSERT INTO t2 VALUES (4, 'Dave', 'Sales')"},
		{"InsertT2_2", "INSERT INTO t2 VALUES (5, 'Eve', 'HR')"},
		{"InsertT2_3", "INSERT INTO t2 VALUES (6, 'Frank', 'Engineering')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	unionTests := []struct {
		name string
		sql  string
	}{
		{"UnionWithWhere1", "SELECT name FROM t1 WHERE dept = 'Engineering' UNION SELECT name FROM t2 WHERE dept = 'Engineering'"},
		{"UnionWithWhere2", "SELECT id, name FROM t1 WHERE id > 1 UNION SELECT id, name FROM t2 WHERE id < 6"},
		{"UnionWithWhere3", "SELECT name FROM t1 WHERE dept = 'Sales' UNION SELECT name FROM t2 WHERE dept = 'Sales'"},
	}

	for _, tt := range unionTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F805_F08105_L1(t *testing.T) {
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
		{"CreateT2", "CREATE TABLE t2 (c INTEGER)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'one')"},
		{"InsertT1_2", "INSERT INTO t1 VALUES (2, 'two')"},
		{"InsertT2", "INSERT INTO t2 VALUES (10)"},
		{"InsertT2_2", "INSERT INTO t2 VALUES (20)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	unionTests := []struct {
		name string
		sql  string
	}{
		{"UnionDifferentCols", "SELECT a, b FROM t1 UNION SELECT c, NULL FROM t2"},
		{"UnionSameCol", "SELECT a FROM t1 UNION SELECT c FROM t2 ORDER BY a"},
	}

	for _, tt := range unionTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
