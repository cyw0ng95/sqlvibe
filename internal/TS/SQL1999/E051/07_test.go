package E051

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E05107_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b TEXT, c INTEGER)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'x', 100), (2, 'y', 200), (1, 'z', 150), (3, 'x', 300)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	distinctGroupByTests := []struct {
		name string
		sql  string
	}{
		{"DistinctBasic", "SELECT DISTINCT b FROM t1"},
		{"DistinctMultiple", "SELECT DISTINCT a, b FROM t1"},
		{"GroupByBasic", "SELECT b, COUNT(*) FROM t1 GROUP BY b"},
		{"GroupByNotInSelect", "SELECT COUNT(*) FROM t1 GROUP BY a"},
		{"GroupByExpression", "SELECT a + 1 AS plus_one, COUNT(*) FROM t1 GROUP BY a + 1"},
		{"GroupByWithHaving", "SELECT b, COUNT(*) AS cnt FROM t1 GROUP BY b HAVING COUNT(*) > 1"},
	}

	for _, tt := range distinctGroupByTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E05108_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b TEXT)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'hello'), (2, 'world')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	aliasQualifiedStarTests := []struct {
		name string
		sql  string
	}{
		{"ColumnAlias", "SELECT a AS id FROM t1"},
		{"ColumnAliasWithExpression", "SELECT a * 10 AS doubled FROM t1"},
		{"QualifiedStar", "SELECT t1.* FROM t1"},
		{"TableAlias", "SELECT t.* FROM t1 AS t"},
		{"AliasWithExpression", "SELECT a, a + 1 AS next FROM t1"},
		{"MultipleAliases", "SELECT a AS id, b AS name FROM t1"},
	}

	for _, tt := range aliasQualifiedStarTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E05109_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b TEXT)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	derivedTableTests := []struct {
		name string
		sql  string
	}{
		{"DerivedTableBasic", "SELECT * FROM (SELECT * FROM t1) AS dt"},
		{"DerivedTableWithAlias", "SELECT dt.a FROM (SELECT a FROM t1) AS dt"},
		{"DerivedTableWithWhere", "SELECT * FROM (SELECT * FROM t1 WHERE a > 1) AS dt"},
		{"DerivedTableMultiple", "SELECT * FROM (SELECT * FROM t1) AS a, (SELECT * FROM t1) AS b"},
		{"NestedDerived", "SELECT * FROM (SELECT * FROM (SELECT * FROM t1) AS inner_dt) AS outer_dt"},
	}

	for _, tt := range derivedTableTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E05110_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b INTEGER, c TEXT)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 10, 'x'), (2, 20, 'y'), (3, 30, 'z'), (4, 40, 'x')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	havingTests := []struct {
		name string
		sql  string
	}{
		{"HavingBasic", "SELECT c, COUNT(*) FROM t1 GROUP BY c HAVING COUNT(*) > 1"},
		{"HavingWithAggregate", "SELECT c, SUM(b) FROM t1 GROUP BY c HAVING SUM(b) > 20"},
		{"HavingWithGroupBy", "SELECT a, COUNT(*) FROM t1 GROUP BY a HAVING a > 1"},
		{"HavingWithoutGroup", "SELECT 'test', COUNT(*) FROM t1 HAVING COUNT(*) > 2"},
	}

	for _, tt := range havingTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
