package N015

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_N015_CastNull_L1(t *testing.T) {
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

	queryTests := []struct {
		name string
		sql  string
	}{
		{"CastNullAsInteger", "SELECT CAST(NULL AS INTEGER)"},
		{"CastNullAsText", "SELECT CAST(NULL AS TEXT)"},
		{"CastNullAsReal", "SELECT CAST(NULL AS REAL)"},
		{"CastNullAsBlob", "SELECT CAST(NULL AS BLOB)"},
		{"CastNullIsNull", "SELECT CAST(NULL AS INTEGER) IS NULL"},
		{"CastIntAsInteger", "SELECT CAST(42 AS INTEGER)"},
		{"CastTextAsText", "SELECT CAST('hello' AS TEXT)"},
		{"CastIntAsReal", "SELECT CAST(3 AS REAL)"},
		{"CastRealAsInteger", "SELECT CAST(3.9 AS INTEGER)"},
		{"CastTextAsInteger", "SELECT CAST('123' AS INTEGER)"},
	}
	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
