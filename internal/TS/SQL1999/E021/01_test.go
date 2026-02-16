package E021

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02101_L1(t *testing.T) {
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

	createTests := []struct {
		name string
		sql  string
	}{
		{"CHAR", "CREATE TABLE t1 (a CHAR(10))"},
		{"CHARACTER", "CREATE TABLE t2 (a CHARACTER(10))"},
		{"CHAR_EMPTY", "CREATE TABLE t3 (a CHAR)"},
		{"CHARACTER_EMPTY", "CREATE TABLE t4 (a CHARACTER)"},
		{"CHAR_5", "CREATE TABLE t5 (a CHAR(5))"},
		{"CHAR_1", "CREATE TABLE t6 (a CHAR(1))"},
		{"CHAR_50", "CREATE TABLE t7 (a CHAR(50))"},
		{"AllCharTypes", "CREATE TABLE t8 (a CHAR(5), b CHARACTER(10), c CHAR)"},
		{"CharWithPK", "CREATE TABLE t9 (id INTEGER PRIMARY KEY, name CHAR(20))"},
		{"CharMultiple", "CREATE TABLE t10 (a CHAR(10), b CHAR(20), c CHAR(5))"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE chars (id INTEGER PRIMARY KEY, val CHAR(10))")
	sqliteDB.Exec("CREATE TABLE chars (id INTEGER PRIMARY KEY, val CHAR(10))")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"Short", "INSERT INTO chars VALUES (1, 'hello')"},
		{"Exact", "INSERT INTO chars VALUES (2, '1234567890')"},
		{"Long", "INSERT INTO chars VALUES (3, 'this is longer than 10')"},
		{"Empty", "INSERT INTO chars VALUES (4, '')"},
		{"Numbers", "INSERT INTO chars VALUES (5, '12345')"},
		{"Spaces", "INSERT INTO chars VALUES (6, '  test  ')"},
		{"Special", "INSERT INTO chars VALUES (7, '!@#$%^')"},
		{"Unicode", "INSERT INTO chars VALUES (8, '你好')"},
		{"Null", "INSERT INTO chars VALUES (9, NULL)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM chars ORDER BY id", "VerifyChars")

	selectTests := []struct {
		name string
		sql  string
	}{
		{"SelectChar", "SELECT val FROM chars WHERE id = 1"},
		{"SelectEmpty", "SELECT val FROM chars WHERE id = 4"},
		{"SelectNull", "SELECT val FROM chars WHERE id = 9"},
		{"CharLength", "SELECT LENGTH(val) FROM chars WHERE id = 1"},
		{"CharUpper", "SELECT UPPER(val) FROM chars WHERE id = 1"},
		{"CharConcat", "SELECT val || 'suffix' FROM chars WHERE id = 1"},
	}

	for _, tt := range selectTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
