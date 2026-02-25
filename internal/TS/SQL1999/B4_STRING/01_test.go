package B4_STRING

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
	_ "github.com/glebarez/go-sqlite"
)

func setup(t *testing.T) (*sqlvibe.Database, *sql.DB) {
	t.Helper()
	sv, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	sl, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return sv, sl
}

// TestSQL1999_B4_EmptyString_L1 tests empty string behavior.
func TestSQL1999_B4_EmptyString_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (s TEXT)")
	sl.Exec("CREATE TABLE t (s TEXT)")
	sv.Exec("INSERT INTO t VALUES ('')")
	sl.Exec("INSERT INTO t VALUES ('')")
	sv.Exec("INSERT INTO t VALUES ('hello')")
	sl.Exec("INSERT INTO t VALUES ('hello')")
	sv.Exec("INSERT INTO t VALUES (NULL)")
	sl.Exec("INSERT INTO t VALUES (NULL)")

	tests := []struct{ name, sql string }{
		{"EmptyStringLen", "SELECT LENGTH(s) FROM t ORDER BY s"},
		{"EmptyStringIsNull", "SELECT s IS NULL FROM t ORDER BY s"},
		{"EmptyStringEq", "SELECT s = '' FROM t"},
		{"LengthEmpty", "SELECT LENGTH('')"},
		{"LengthNull", "SELECT LENGTH(NULL)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B4_Whitespace_L1 tests whitespace string behavior.
func TestSQL1999_B4_Whitespace_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"TrimBoth", "SELECT TRIM('  hello  ')"},
		{"TrimLeading", "SELECT LTRIM('  hello')"},
		{"TrimTrailing", "SELECT RTRIM('hello  ')"},
		{"LengthSpaces", "SELECT LENGTH('   ')"},
		{"TrimChars", "SELECT TRIM('xxhelloxx', 'x')"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B4_LikeBasic_L1 tests LIKE pattern matching.
func TestSQL1999_B4_LikeBasic_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (s TEXT)")
	sl.Exec("CREATE TABLE t (s TEXT)")
	for _, v := range []string{"'hello'", "'world'", "'help'", "'HELLO'", "'a%b'"} {
		sv.Exec("INSERT INTO t VALUES (" + v + ")")
		sl.Exec("INSERT INTO t VALUES (" + v + ")")
	}

	tests := []struct{ name, sql string }{
		{"LikePercent", "SELECT s FROM t WHERE s LIKE 'hel%' ORDER BY s"},
		{"LikeUnderscore", "SELECT s FROM t WHERE s LIKE 'hel__' ORDER BY s"},
		{"LikeNotMatch", "SELECT COUNT(*) FROM t WHERE s LIKE 'xyz%'"},
		{"NotLike", "SELECT s FROM t WHERE s NOT LIKE 'hel%' ORDER BY s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B4_LikeEscape_L1 tests LIKE with ESCAPE clause.
func TestSQL1999_B4_LikeEscape_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (s TEXT)")
	sl.Exec("CREATE TABLE t (s TEXT)")
	sv.Exec("INSERT INTO t VALUES ('a%b')")
	sl.Exec("INSERT INTO t VALUES ('a%b')")
	sv.Exec("INSERT INTO t VALUES ('axb')")
	sl.Exec("INSERT INTO t VALUES ('axb')")
	sv.Exec("INSERT INTO t VALUES ('a_b')")
	sl.Exec("INSERT INTO t VALUES ('a_b')")

	tests := []struct{ name, sql string }{
		{"EscapePercent", "SELECT s FROM t WHERE s LIKE 'a\\%b' ESCAPE '\\'"},
		{"EscapeUnderscore", "SELECT s FROM t WHERE s LIKE 'a\\_b' ESCAPE '\\'"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B4_StringFunctions_L1 tests common string functions.
func TestSQL1999_B4_StringFunctions_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"Upper", "SELECT UPPER('hello')"},
		{"Lower", "SELECT LOWER('WORLD')"},
		{"Substr", "SELECT SUBSTR('hello', 2, 3)"},
		{"Replace", "SELECT REPLACE('hello world', 'world', 'there')"},
		{"Instr", "SELECT INSTR('hello', 'ell')"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B4_Glob_L1 tests GLOB operator (case-sensitive).
func TestSQL1999_B4_Glob_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	sv.Exec("CREATE TABLE t (s TEXT)")
	sl.Exec("CREATE TABLE t (s TEXT)")
	for _, v := range []string{"'hello'", "'Hello'", "'world'", "'help'"} {
		sv.Exec("INSERT INTO t VALUES (" + v + ")")
		sl.Exec("INSERT INTO t VALUES (" + v + ")")
	}

	tests := []struct{ name, sql string }{
		{"GlobStar", "SELECT s FROM t WHERE s GLOB 'hel*' ORDER BY s"},
		{"GlobCase", "SELECT s FROM t WHERE s GLOB 'Hel*' ORDER BY s"},
		{"GlobQuestion", "SELECT s FROM t WHERE s GLOB 'hel??' ORDER BY s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_B4_StringLength_L1 tests LENGTH function edge cases.
func TestSQL1999_B4_StringLength_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()

	tests := []struct{ name, sql string }{
		{"LengthHello", "SELECT LENGTH('hello')"},
		{"LengthUnicode", "SELECT LENGTH('héllo')"},
		{"LengthEmpty", "SELECT LENGTH('')"},
		{"LengthNull", "SELECT LENGTH(NULL)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}
