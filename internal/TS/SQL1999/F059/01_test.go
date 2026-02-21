package F059

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F059_NullComparisons_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE t (id INTEGER, val TEXT)"},
		{"Insert1", "INSERT INTO t VALUES (1, 'hello')"},
		{"Insert2", "INSERT INTO t VALUES (2, NULL)"},
		{"Insert3", "INSERT INTO t VALUES (3, 'world')"},
		{"Insert4", "INSERT INTO t VALUES (4, NULL)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"IsNull", "SELECT id FROM t WHERE val IS NULL ORDER BY id"},
		{"IsNotNull", "SELECT id FROM t WHERE val IS NOT NULL ORDER BY id"},
		{"CountAll", "SELECT COUNT(*) FROM t"},
		{"CountNonNull", "SELECT COUNT(val) FROM t"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F059_CoalesceNullif_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE data (id INTEGER, a TEXT, b TEXT)"},
		{"Insert1", "INSERT INTO data VALUES (1, NULL, 'fallback')"},
		{"Insert2", "INSERT INTO data VALUES (2, 'primary', 'fallback')"},
		{"Insert3", "INSERT INTO data VALUES (3, NULL, NULL)"},
		{"Insert4", "INSERT INTO data VALUES (4, 'value', NULL)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"CoalesceAB", "SELECT id, COALESCE(a, b) AS result FROM data ORDER BY id"},
		{"CoalesceNull", "SELECT COALESCE(NULL, 'second') AS result"},
		{"CoalesceNoNull", "SELECT COALESCE('first', 'second') AS result"},
		{"NullifEqual", "SELECT NULLIF(1, 1) AS result"},
		{"NullifNotEqual", "SELECT NULLIF(1, 2) AS result"},
		{"NullifString", "SELECT id, NULLIF(a, 'primary') AS result FROM data ORDER BY id"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F059_StringConcatAndLike_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE words (id INTEGER, word TEXT)"},
		{"Insert1", "INSERT INTO words VALUES (1, 'apple')"},
		{"Insert2", "INSERT INTO words VALUES (2, 'apricot')"},
		{"Insert3", "INSERT INTO words VALUES (3, 'banana')"},
		{"Insert4", "INSERT INTO words VALUES (4, 'blueberry')"},
		{"Insert5", "INSERT INTO words VALUES (5, 'cherry')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"StringConcat", "SELECT id, 'fruit: ' || word AS label FROM words ORDER BY id"},
		{"LikePercent", "SELECT word FROM words WHERE word LIKE 'a%' ORDER BY word"},
		{"LikeUnderscore", "SELECT word FROM words WHERE word LIKE 'c_erry' ORDER BY word"},
		{"LikeMiddle", "SELECT word FROM words WHERE word LIKE '%erry' ORDER BY word"},
		{"NotLike", "SELECT word FROM words WHERE word NOT LIKE 'a%' ORDER BY word"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F059_CaseExpression_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE grades (id INTEGER, score INTEGER)"},
		{"Insert1", "INSERT INTO grades VALUES (1, 95)"},
		{"Insert2", "INSERT INTO grades VALUES (2, 82)"},
		{"Insert3", "INSERT INTO grades VALUES (3, 73)"},
		{"Insert4", "INSERT INTO grades VALUES (4, 61)"},
		{"Insert5", "INSERT INTO grades VALUES (5, 45)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SearchedCase", "SELECT id, score, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' WHEN score >= 60 THEN 'D' ELSE 'F' END AS grade FROM grades ORDER BY id"},
		{"SimpleCase", "SELECT id, CASE id WHEN 1 THEN 'first' WHEN 2 THEN 'second' ELSE 'other' END AS label FROM grades ORDER BY id"},
		{"CaseInWhere", "SELECT id FROM grades WHERE CASE WHEN score >= 60 THEN 1 ELSE 0 END = 1 ORDER BY id"},
		{"CaseCount", "SELECT COUNT(*) FROM grades WHERE score >= 60"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F059_TypeCoercionAndEdgeCases_L1(t *testing.T) {
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
		{"CreateTable", "CREATE TABLE mixed (id INTEGER, ival INTEGER, fval REAL, sval TEXT)"},
		{"Insert1", "INSERT INTO mixed VALUES (1, 10, 3.14, 'hello')"},
		{"Insert2", "INSERT INTO mixed VALUES (2, 0, 0.0, '')"},
		{"Insert3", "INSERT INTO mixed VALUES (3, 9223372036854775807, 1.5e10, 'end')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"IntPlusFloat", "SELECT id, ival + 0.0 AS result FROM mixed ORDER BY id"},
		{"LargeInt", "SELECT ival FROM mixed WHERE id = 3"},
		{"EmptyStringNotNull", "SELECT COUNT(*) FROM mixed WHERE sval = ''"},
		{"EmptyStringIsNotNull", "SELECT COUNT(*) FROM mixed WHERE sval IS NOT NULL"},
		{"ZeroIsFalsy", "SELECT COUNT(*) FROM mixed WHERE ival = 0"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
