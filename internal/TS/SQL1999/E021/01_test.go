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
		{"AllCharTypes", "CREATE TABLE t5 (a CHAR(5), b CHARACTER(10), c CHAR)"},
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
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM chars ORDER BY id", "VerifyChars")
}
