package E061

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E06109_L1(t *testing.T) {
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
		{"InsertValues", "INSERT INTO t1 VALUES (1, 10, 'test'), (2, 20, 'hello'), (3, 30, 'world'), (4, 40, 'test'), (5, 50, NULL)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	betweenTests := []struct {
		name string
		sql  string
	}{
		{"BetweenBasic", "SELECT * FROM t1 WHERE a BETWEEN 2 AND 4"},
		{"BetweenNot", "SELECT * FROM t1 WHERE a NOT BETWEEN 2 AND 4"},
		{"BetweenEqual", "SELECT * FROM t1 WHERE a BETWEEN 2 AND 2"},
		{"BetweenReverse", "SELECT * FROM t1 WHERE a BETWEEN 4 AND 2"},
		{"BetweenNull", "SELECT * FROM t1 WHERE a BETWEEN NULL AND 5"},
		{"BetweenText", "SELECT * FROM t1 WHERE c BETWEEN 'a' AND 'z'"},
	}

	for _, tt := range betweenTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	inTests := []struct {
		name string
		sql  string
	}{
		{"InList", "SELECT * FROM t1 WHERE a IN (1, 2, 3)"},
		{"InNot", "SELECT * FROM t1 WHERE a NOT IN (1, 2, 3)"},
		{"InSingle", "SELECT * FROM t1 WHERE a IN (2)"},
		{"InEmpty", "SELECT * FROM t1 WHERE a IN ()"},
		{"InText", "SELECT * FROM t1 WHERE c IN ('test', 'hello')"},
		{"InNull", "SELECT * FROM t1 WHERE a IN (NULL, 1)"},
	}

	for _, tt := range inTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E06110_L1(t *testing.T) {
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
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, name TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, value INTEGER)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, NULL)"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 100), (2, 200), (5, 500), (6, NULL)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	existsTests := []struct {
		name string
		sql  string
	}{
		{"ExistsBasic", "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2)"},
		{"NotExists", "SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)"},
		{"ExistsWithCondition", "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.value > 100)"},
		{"ExistsCorrelated", "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)"},
		{"ExistsWithNull", "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id IS NULL)"},
	}

	for _, tt := range existsTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	inSubqueryTests := []struct {
		name string
		sql  string
	}{
		{"InSubquery", "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2)"},
		{"NotInSubquery", "SELECT * FROM t1 WHERE id NOT IN (SELECT id FROM t2)"},
		{"InSubqueryWithNull", "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2 WHERE id IS NOT NULL)"},
	}

	for _, tt := range inSubqueryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E06111_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (id INTEGER, name TEXT, code TEXT)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 'apple', '100%'), (2, 'banana', 'abc'), (3, 'cherry', 'test_'), (4, 'date', 'a_c'), (5, 'fig', 'hello'), (6, NULL, 'null')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	likeTests := []struct {
		name string
		sql  string
	}{
		{"LikePercentStart", "SELECT * FROM t1 WHERE name LIKE '%apple'"},
		{"LikePercentEnd", "SELECT * FROM t1 WHERE name LIKE 'a%'"},
		{"LikePercentBoth", "SELECT * FROM t1 WHERE name LIKE '%pp%'"},
		{"LikeUnderscore", "SELECT * FROM t1 WHERE code LIKE '___'"},
		{"LikeUnderscoreMultiple", "SELECT * FROM t1 WHERE code LIKE '_____%'"},
		{"LikeEscape", "SELECT * FROM t1 WHERE code LIKE '100%' ESCAPE '%'"},
		{"LikeNoMatch", "SELECT * FROM t1 WHERE name LIKE 'xyz%'"},
		{"LikeCaseInsensitive", "SELECT * FROM t1 WHERE name LIKE 'APPLE'"},
		{"LikeNull", "SELECT * FROM t1 WHERE name LIKE '%'"},
	}

	for _, tt := range likeTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E06112_L1(t *testing.T) {
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
		{"CreateT1", "CREATE TABLE t1 (a INTEGER, b INTEGER)"},
		{"CreateT2", "CREATE TABLE t2 (x INTEGER, y INTEGER)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30), (100, 5)"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 100), (2, 200), (3, 300)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	quantifiedTests := []struct {
		name string
		sql  string
	}{
		{"AllGreater", "SELECT * FROM t1 WHERE a > ALL (SELECT x FROM t2)"},
		{"AllLess", "SELECT * FROM t1 WHERE a < ALL (SELECT x FROM t2)"},
		{"AnyEqual", "SELECT * FROM t1 WHERE a = ANY (SELECT x FROM t2)"},
		{"AnyGreater", "SELECT * FROM t1 WHERE a > ANY (SELECT x FROM t2)"},
		{"SomeLess", "SELECT * FROM t1 WHERE a < SOME (SELECT x FROM t2)"},
		{"AllWithNull", "SELECT * FROM t1 WHERE a > ALL (SELECT x FROM t2 WHERE x IS NOT NULL)"},
	}

	for _, tt := range quantifiedTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E06113_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)"},
		{"InsertValues", "INSERT INTO t1 VALUES (1, 1, 10), (2, 1, 20), (3, 2, 30), (4, 2, 40), (5, 3, 50)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	searchConditionTests := []struct {
		name string
		sql  string
	}{
		{"AndBoth", "SELECT * FROM t1 WHERE a > 1 AND b = 1"},
		{"OrBoth", "SELECT * FROM t1 WHERE a = 1 OR a = 3"},
		{"NotAnd", "SELECT * FROM t1 WHERE NOT (a > 1 AND b = 1)"},
		{"AndOr", "SELECT * FROM t1 WHERE a > 1 AND b = 1 OR a = 5"},
		{"AndNotOr", "SELECT * FROM t1 WHERE a > 1 AND (b = 1 OR b = 2)"},
		{"NotIn", "SELECT * FROM t1 WHERE NOT a IN (1, 2, 3)"},
		{"NotLike", "SELECT * FROM t1 WHERE NOT a LIKE '1%'"},
		{"NotBetween", "SELECT * FROM t1 WHERE a NOT BETWEEN 2 AND 4"},
		{"AndWithNull", "SELECT * FROM t1 WHERE a > 1 AND b IS NOT NULL"},
		{"ComplexCondition", "SELECT * FROM t1 WHERE (a > 1 AND b = 1) OR (a < 5 AND c > 30)"},
	}

	for _, tt := range searchConditionTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
