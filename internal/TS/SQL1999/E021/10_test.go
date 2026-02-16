package E021

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02110_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE casting_test (id INTEGER PRIMARY KEY, str TEXT, num INTEGER)")
	sqliteDB.Exec("CREATE TABLE casting_test (id INTEGER PRIMARY KEY, str TEXT, num INTEGER)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"IntToText", "INSERT INTO casting_test VALUES (1, '10', 10)"},
		{"TextToInt", "INSERT INTO casting_test VALUES (2, '20', 20)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	castTests := []struct {
		name string
		sql  string
	}{
		{"ConcatIntText", "SELECT num || ' is a number' FROM casting_test WHERE id = 1"},
		{"ConcatTextInt", "SELECT str || 100 FROM casting_test WHERE id = 1"},
		{"IntComparison", "SELECT num = 10 FROM casting_test WHERE id = 1"},
		{"TextComparison", "SELECT str = '10' FROM casting_test WHERE id = 1"},
		{"ImplicitIntInConcat", "SELECT 'Value: ' || num FROM casting_test WHERE id = 1"},
	}

	for _, tt := range castTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
