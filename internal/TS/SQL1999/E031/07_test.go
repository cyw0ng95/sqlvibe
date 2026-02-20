package E031

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E03107_L1(t *testing.T) {
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
		{"DelimitedIdentifier", "CREATE TABLE \"MyTable\" (id INTEGER, name TEXT)"},
		{"DelimitedKeyword", "CREATE TABLE \"Select\" (id INTEGER, value TEXT)"},
		{"DelimitedWithSpaces", "CREATE TABLE \"Table With Spaces\" (id INTEGER, data TEXT)"},
		{"MixedCase", "CREATE TABLE MyTable (id INTEGER, name TEXT)"},
		{"LowerCase", "CREATE TABLE mytable (id INTEGER, name TEXT)"},
		{"UpperCase", "CREATE TABLE MYTABLE (id INTEGER, name TEXT)"},
		{"UnderscoreName", "CREATE TABLE my_table (id INTEGER, name TEXT)"},
		{"TrailingUnderscore", "CREATE TABLE table_ (id INTEGER, name TEXT)"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	insertTests := []struct {
		name string
		sql  string
	}{
		{"InsertDelimited", "INSERT INTO \"MyTable\" VALUES (1, 'test')"},
		{"InsertKeyword", "INSERT INTO \"Select\" VALUES (1, 'value')"},
		{"InsertWithSpaces", "INSERT INTO \"Table With Spaces\" VALUES (1, 'data')"},
		{"InsertMixedCase", "INSERT INTO MyTable VALUES (1, 'test')"},
		{"InsertLowerCase", "INSERT INTO mytable VALUES (2, 'test2')"},
		{"InsertUpperCase", "INSERT INTO MYTABLE VALUES (3, 'test3')"},
		{"InsertUnderscore", "INSERT INTO my_table VALUES (4, 'test4')"},
		{"InsertTrailingUnderscore", "INSERT INTO table_ VALUES (5, 'test5')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectDelimited", "SELECT * FROM \"MyTable\""},
		{"SelectKeyword", "SELECT * FROM \"Select\""},
		{"SelectWithSpaces", "SELECT * FROM \"Table With Spaces\""},
		{"SelectMixedCase", "SELECT * FROM MyTable"},
		{"SelectLowerCase", "SELECT * FROM mytable"},
		{"SelectUpperCase", "SELECT * FROM MYTABLE"},
		{"SelectUnderscore", "SELECT * FROM my_table"},
		{"SelectTrailingUnderscore", "SELECT * FROM table_"},
		{"WhereDelimited", "SELECT * FROM \"MyTable\" WHERE id = 1"},
		{"WhereKeyword", "SELECT * FROM \"Select\" WHERE value = 'value'"},
		{"OrderByDelimited", "SELECT * FROM \"MyTable\" ORDER BY name"},
		{"CaseInsensitive", "SELECT * FROM MyTable WHERE name = 'test'"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
