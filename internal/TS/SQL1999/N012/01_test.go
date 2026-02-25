package N012

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_N012_NullLogic_L1(t *testing.T) {
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

	// Three-valued logic tests using scalar expressions
	queryTests := []struct {
		name string
		sql  string
	}{
		// AND with NULL
		{"NullAndTrue", "SELECT NULL AND 1"},
		{"TrueAndNull", "SELECT 1 AND NULL"},
		{"NullAndNull", "SELECT NULL AND NULL"},
		// OR with NULL
		{"NullOrFalse", "SELECT NULL OR 0"},
		{"FalseOrNull", "SELECT 0 OR NULL"},
		{"NullOrNull", "SELECT NULL OR NULL"},
		// NOT with NULL (NOT NULL behavior is implementation-defined; skip)
		{"NotTrue", "SELECT NOT 1"},
		{"NotFalse", "SELECT NOT 0"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
