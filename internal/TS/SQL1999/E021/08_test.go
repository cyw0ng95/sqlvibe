package E021

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E02108_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE case_test (id INTEGER PRIMARY KEY, val TEXT)")
	sqliteDB.Exec("CREATE TABLE case_test (id INTEGER PRIMARY KEY, val TEXT)")

	insertTests := []struct {
		name string
		sql  string
	}{
		{"Lower", "INSERT INTO case_test VALUES (1, 'hello')"},
		{"Upper", "INSERT INTO case_test VALUES (2, 'HELLO')"},
		{"Mixed", "INSERT INTO case_test VALUES (3, 'HeLLo WoRLd')"},
		{"Empty", "INSERT INTO case_test VALUES (4, '')"},
		{"Numbers", "INSERT INTO case_test VALUES (5, '123abc456')"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	caseTests := []struct {
		name string
		sql  string
	}{
		{"UPPER", "SELECT UPPER(val) FROM case_test WHERE id = 1"},
		{"UPPER_Mixed", "SELECT UPPER(val) FROM case_test WHERE id = 3"},
		{"UPPER_Empty", "SELECT UPPER(val) FROM case_test WHERE id = 4"},
		{"UPPER_Literal", "SELECT UPPER('hello world')"},
		{"LOWER", "SELECT LOWER(val) FROM case_test WHERE id = 2"},
		{"LOWER_Mixed", "SELECT LOWER(val) FROM case_test WHERE id = 3"},
		{"LOWER_Empty", "SELECT LOWER(val) FROM case_test WHERE id = 4"},
		{"LOWER_Literal", "SELECT LOWER('HELLO WORLD')"},
		{"UPPER_LOWER", "SELECT UPPER(LOWER(val)) FROM case_test WHERE id = 3"},
		{"LOWER_UPPER", "SELECT LOWER(UPPER(val)) FROM case_test WHERE id = 1"},
	}

	for _, tt := range caseTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
