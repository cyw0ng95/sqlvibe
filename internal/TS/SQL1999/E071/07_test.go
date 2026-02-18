package E071

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E07107_L1(t *testing.T) {
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
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, name TEXT, category TEXT, price REAL)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, value INTEGER)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'apple', 'fruit', 1.99), (2, 'banana', 'fruit', 0.99), (3, 'carrot', 'vegetable', 1.50), (4, 'donut', 'snack', 2.50)"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 100), (2, 200), (3, 300), (5, 500)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	correlatedSubqueryTests := []struct {
		name string
		sql  string
	}{
		{"CorrelatedExists", "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.id = t1.id)"},
		{"CorrelatedIn", "SELECT * FROM t1 WHERE id IN (SELECT id FROM t2 WHERE t2.value > 100)"},
		{"CorrelatedComparison", "SELECT * FROM t1 WHERE price > (SELECT AVG(price) FROM t1 WHERE category = t1.category)"},
		{"CorrelatedScalar", "SELECT name, (SELECT MAX(value) FROM t2 WHERE t2.id <= t1.id) AS running_max FROM t1"},
	}

	for _, tt := range correlatedSubqueryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E07108_L1(t *testing.T) {
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
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, x INTEGER)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, y INTEGER)"},
		{"CreateT3", "CREATE TABLE t3 (id INTEGER, z INTEGER)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30)"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 100), (2, 200), (3, 300)"},
		{"InsertT3", "INSERT INTO t3 VALUES (1, 1000), (2, 2000), (3, 3000)"},
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
		{"DerivedTableWithAlias", "SELECT dt.x FROM (SELECT x FROM t1) AS dt"},
		{"DerivedTableMultiple", "SELECT * FROM (SELECT * FROM t1) AS a, (SELECT * FROM t2) AS b"},
		{"DerivedTableJoin", "SELECT * FROM (SELECT * FROM t1) AS a JOIN (SELECT * FROM t2) AS b ON a.id = b.id"},
		{"DerivedTableWithWhere", "SELECT * FROM (SELECT * FROM t1 WHERE x > 10) AS dt"},
		{"NestedDerivedTable", "SELECT * FROM (SELECT * FROM (SELECT * FROM t1) AS inner_dt) AS outer_dt"},
	}

	for _, tt := range derivedTableTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E07109_L1(t *testing.T) {
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
		{"CreateT1", "CREATE TABLE t1 (a INTEGER, b INTEGER, c INTEGER)"},
		{"CreateT2", "CREATE TABLE t2 (x INTEGER, y INTEGER)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 10, 100), (2, 20, 200), (3, 30, 300), (4, 40, 400)"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 1000), (2, 2000), (5, 5000)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	existsNotExistsTests := []struct {
		name string
		sql  string
	}{
		{"ExistsSimple", "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2)"},
		{"NotExistsSimple", "SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2)"},
		{"ExistsCorrelated", "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.x = t1.a)"},
		{"NotExistsCorrelated", "SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.x = t1.a)"},
		{"ExistsWithCondition", "SELECT * FROM t1 WHERE EXISTS (SELECT 1 FROM t2 WHERE t2.y > 1500)"},
		{"NotExistsWithCondition", "SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.y > 1500)"},
		{"ExistsNull", "SELECT * FROM t1 WHERE EXISTS (SELECT NULL)"},
	}

	for _, tt := range existsNotExistsTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E07110_L1(t *testing.T) {
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
		{"CreateT1", "CREATE TABLE t1 (a INTEGER)"},
		{"CreateT2", "CREATE TABLE t2 (b INTEGER)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1), (2), (3), (10)"},
		{"InsertT2", "INSERT INTO t2 VALUES (2), (3), (4), (5)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	notInSubqueryTests := []struct {
		name string
		sql  string
	}{
		{"NotInBasic", "SELECT * FROM t1 WHERE a NOT IN (SELECT b FROM t2)"},
		{"NotInWithNulls", "SELECT * FROM t1 WHERE a NOT IN (SELECT b FROM t2 WHERE b IS NOT NULL)"},
		{"NotInEmpty", "SELECT * FROM t1 WHERE a NOT IN (SELECT b FROM t2 WHERE b > 100)"},
	}

	for _, tt := range notInSubqueryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E07111_L1(t *testing.T) {
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
		{"CreateT1", "CREATE TABLE t1 (a INTEGER)"},
		{"CreateT2", "CREATE TABLE t2 (b INTEGER)"},
		{"CreateT3", "CREATE TABLE t3 (c INTEGER)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1), (2), (3), (4)"},
		{"InsertT2", "INSERT INTO t2 VALUES (5), (6), (7)"},
		{"InsertT3", "INSERT INTO t3 VALUES (1), (3), (5), (7), (9)"},
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
		{"AllGreaterThan", "SELECT * FROM t1 WHERE a > ALL (SELECT b FROM t2)"},
		{"AllLessThan", "SELECT * FROM t1 WHERE a < ALL (SELECT b FROM t2)"},
		{"AllEqual", "SELECT * FROM t1 WHERE a = ALL (SELECT b FROM t2 WHERE b < 10)"},
		{"AnyEqual", "SELECT * FROM t1 WHERE a = ANY (SELECT c FROM t3)"},
		{"AnyGreaterThan", "SELECT * FROM t1 WHERE a > ANY (SELECT b FROM t2)"},
		{"SomeEqual", "SELECT * FROM t1 WHERE a = SOME (SELECT c FROM t3)"},
		{"AllWithNull", "SELECT * FROM t1 WHERE a > ALL (SELECT b FROM t2 WHERE b IS NOT NULL)"},
	}

	for _, tt := range quantifiedTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F301_E07112_L1(t *testing.T) {
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
		{"InsertT1", "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	nestedSubqueryTests := []struct {
		name string
		sql  string
	}{
		{"NestedTwoLevels", "SELECT * FROM t1 WHERE a > (SELECT AVG(a) FROM t1 WHERE a > (SELECT MIN(a) FROM t1))"},
		{"NestedThreeLevels", "SELECT * FROM t1 WHERE a = (SELECT x FROM (SELECT a AS x FROM t1 WHERE a > (SELECT MIN(a) FROM t1)) AS dt WHERE x > 2)"},
		{"SubqueryWithLimit", "SELECT * FROM t1 WHERE a IN (SELECT a FROM t1 ORDER BY a DESC LIMIT 2)"},
		{"SubqueryInSelect", "SELECT a, (SELECT MAX(a) FROM t1) AS max_a, (SELECT MIN(a) FROM t1) AS min_a FROM t1"},
		{"SubqueryInFromWithLimit", "SELECT * FROM (SELECT * FROM t1 ORDER BY a LIMIT 3) AS dt"},
	}

	for _, tt := range nestedSubqueryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
