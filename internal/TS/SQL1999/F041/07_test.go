package F041

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F407_F04107_L1(t *testing.T) {
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
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 100), (2, 200), (4, 400)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	innerJoinTests := []struct {
		name string
		sql  string
	}{
		{"InnerJoinBasic", "SELECT t1.name, t2.value FROM t1 INNER JOIN t2 ON t1.id = t2.id"},
		{"InnerJoinWithUsing", "SELECT name, value FROM t1 INNER JOIN t2 USING (id)"},
		{"InnerJoinMultiple", "SELECT t1.id, t1.name, t2.value FROM t1 INNER JOIN t2 ON t1.id = t2.id WHERE t2.value > 100"},
		{"InnerJoinComplexOn", "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id AND t2.value > 150"},
	}

	for _, tt := range innerJoinTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F408_F04108_L1(t *testing.T) {
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
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c'), (5, 'e')"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 100), (2, 200), (4, 400)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	leftJoinTests := []struct {
		name string
		sql  string
	}{
		{"LeftJoinBasic", "SELECT t1.name, t2.value FROM t1 LEFT JOIN t2 ON t1.id = t2.id"},
		{"LeftJoinWithUsing", "SELECT name, value FROM t1 LEFT JOIN t2 USING (id)"},
		{"LeftJoinWhereNull", "SELECT t1.name, t2.value FROM t1 LEFT JOIN t2 ON t1.id = t2.id WHERE t2.id IS NULL"},
		{"LeftJoinMultiple", "SELECT t1.id, t1.name, t2.value FROM t1 LEFT JOIN t2 ON t1.id = t2.id WHERE t1.id > 1"},
		{"LeftJoinWithOrderBy", "SELECT t1.name, t2.value FROM t1 LEFT JOIN t2 ON t1.id = t2.id ORDER BY t2.value DESC"},
	}

	for _, tt := range leftJoinTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F409_F04109_L1(t *testing.T) {
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
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (4, 'd')"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 100), (2, 200), (3, 300), (5, 500)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	rightJoinTests := []struct {
		name string
		sql  string
	}{
		{"RightJoinBasic", "SELECT t1.name, t2.value FROM t1 RIGHT JOIN t2 ON t1.id = t2.id"},
		{"RightJoinWithUsing", "SELECT name, value FROM t1 RIGHT JOIN t2 USING (id)"},
		{"RightJoinWhereNull", "SELECT t1.name, t2.value FROM t1 RIGHT JOIN t2 ON t1.id = t2.id WHERE t1.id IS NULL"},
		{"RightJoinMultiple", "SELECT t1.id, t1.name, t2.value FROM t1 RIGHT JOIN t2 ON t1.id = t2.id WHERE t2.value > 150"},
	}

	for _, tt := range rightJoinTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F410_F04110_L1(t *testing.T) {
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
		{"CreateT3", "CREATE TABLE t3 (id INTEGER, code TEXT)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 100), (2, 200), (4, 400)"},
		{"InsertT3", "INSERT INTO t3 VALUES (1, 'X'), (2, 'Y'), (5, 'Z')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	nestedJoinTests := []struct {
		name string
		sql  string
	}{
		{"NestedLeftJoin", "SELECT t1.name, t2.value, t3.code FROM t1 LEFT JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id"},
		{"MixedJoinTypes", "SELECT t1.name, t2.value, t3.code FROM t1 LEFT JOIN t2 ON t1.id = t2.id INNER JOIN t3 ON t1.id = t3.id"},
		{"MultipleLeftJoins", "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id LEFT JOIN t3 ON t2.id = t3.id"},
		{"ThreeWayJoin", "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id"},
	}

	for _, tt := range nestedJoinTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F411_F04111_L1(t *testing.T) {
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
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 100), (2, 200), (3, 300)"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	crossJoinTests := []struct {
		name string
		sql  string
	}{
		{"CrossJoinBasic", "SELECT t1.name, t2.value FROM t1 CROSS JOIN t2"},
		{"ImplicitCommaJoin", "SELECT t1.name, t2.value FROM t1, t2"},
		{"CrossJoinWithWhere", "SELECT t1.name, t2.value FROM t1 CROSS JOIN t2 WHERE t1.id <= 2"},
		{"SelfJoin", "SELECT a.name AS name1, b.name AS name2 FROM t1 AS a, t1 AS b WHERE a.id < b.id"},
		{"SelfJoinExplicit", "SELECT a.name AS name1, b.name AS name2 FROM t1 AS a JOIN t1 AS b ON a.id < b.id"},
	}

	for _, tt := range crossJoinTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F412_F04112_L1(t *testing.T) {
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
		{"CreateT1", "CREATE TABLE t1 (id INTEGER, name TEXT, cat TEXT)"},
		{"CreateT2", "CREATE TABLE t2 (id INTEGER, value INTEGER, cat TEXT)"},
		{"InsertT1", "INSERT INTO t1 VALUES (1, 'a', 'X'), (2, 'b', 'Y'), (3, 'c', 'X')"},
		{"InsertT2", "INSERT INTO t2 VALUES (1, 100, 'X'), (2, 200, 'Y'), (4, 400, 'Z')"},
	}

	for _, tt := range setupTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	complexJoinTests := []struct {
		name string
		sql  string
	}{
		{"JoinComplexOn", "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id AND t1.cat = t2.cat"},
		{"JoinWithAnd", "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id AND t2.value > 150"},
		{"JoinWithOr", "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id OR t2.cat = 'Z'"},
		{"JoinWithOrderBy", "SELECT t1.name, t2.value FROM t1 JOIN t2 ON t1.id = t2.id ORDER BY t2.value DESC"},
		{"JoinWithGroupBy", "SELECT t1.cat, COUNT(*) FROM t1 LEFT JOIN t2 ON t1.id = t2.id GROUP BY t1.cat"},
		{"MultipleJoins", "SELECT * FROM t1 t1a JOIN t2 ON t1a.id = t2.id JOIN t1 t1b ON t1a.cat = t1b.cat"},
		{"AllComparisonOperators", "SELECT * FROM t1 JOIN t2 ON t1.id <= t2.id AND t1.id >= t2.id - 2"},
	}

	for _, tt := range complexJoinTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
