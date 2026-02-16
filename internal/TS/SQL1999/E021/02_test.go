package E021

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02102_L1(t *testing.T) {
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
		{"VARCHAR", "CREATE TABLE t1 (a VARCHAR(255))"},
		{"VARCHAR_LARGE", "CREATE TABLE t2 (a VARCHAR(1000))"},
		{"VARCHAR_SMALL", "CREATE TABLE t3 (a VARCHAR(10))"},
		{"VARCHAR_NO_SIZE", "CREATE TABLE t4 (a TEXT)"},
		{"AllVarcharTypes", "CREATE TABLE t5 (a VARCHAR(50), b VARCHAR(100), c TEXT)"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	sqlvibeDB.Exec("CREATE TABLE varchars (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE varchars (id INTEGER PRIMARY KEY, val TEXT)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"Short", "INSERT INTO varchars VALUES (1, 'hello')"},
		{"Long", "INSERT INTO varchars VALUES (2, 'this is a much longer string that can hold more data')"},
		{"Empty", "INSERT INTO varchars VALUES (3, '')"},
		{"Unicode", "INSERT INTO varchars VALUES (4, 'hello world')"},
		{"Special", "INSERT INTO varchars VALUES (5, 'a b c d e f g h i j k l m n o p q r s t u v w x y z')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "SELECT * FROM varchars ORDER BY id", "VerifyVarchars")
}
