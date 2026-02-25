package F874

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F874_DateTimeFunctions_L1 validates v0.9.2 date/time function fixes.
func TestSQL1999_F874_DateTimeFunctions_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	queries := []struct {
		name string
		sql  string
	}{
		{"JulianFixed5", "SELECT ROUND(julianday('2000-01-01'), 5)"},
		{"JulianDiff", "SELECT ROUND(julianday('2024-01-01') - julianday('2023-01-01'))"},
		{"JulianNow", "SELECT ROUND(julianday('now'), 0) > 2400000"},
		{"JulianNull", "SELECT julianday(NULL)"},
		{"RoundNoDecimals", "SELECT ROUND(2.7)"},
		{"RoundDecimals", "SELECT ROUND(3.14159, 2)"},
		{"RoundNeg", "SELECT ROUND(-2.5)"},
		{"Strftime", "SELECT strftime('%Y', '2024-01-15')"},
		{"UnixEpoch", "SELECT unixepoch('2000-01-01 00:00:00')"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F874_UnknownFunctionError_L1 validates that undefined functions
// produce an error instead of silently returning NULL.
func TestSQL1999_F874_UnknownFunctionError_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE t (x INTEGER)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (42)`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Unknown function in SELECT … FROM should error.
	_, err = db.Query(`SELECT no_such_func(x) FROM t`)
	if err == nil {
		t.Error("expected error for unknown function with FROM, got nil")
	} else if !strings.Contains(err.Error(), "no such function") {
		t.Errorf("expected 'no such function' error, got: %v", err)
	}

	// Unknown function in constant SELECT (no FROM) should error.
	_, err = db.Query(`SELECT no_such_func(1)`)
	if err == nil {
		t.Error("expected error for unknown function without FROM, got nil")
	} else if !strings.Contains(err.Error(), "no such function") {
		t.Errorf("expected 'no such function' error, got: %v", err)
	}
}

// TestSQL1999_F874_MathFunctions_L1 validates math functions in constant SELECT context.
func TestSQL1999_F874_MathFunctions_L1(t *testing.T) {
	sqlvibeDB, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	sqliteDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("Failed to open sqlite: %v", err)
	}
	defer sqliteDB.Close()

	queries := []struct {
		name string
		sql  string
	}{
		{"Abs", "SELECT ABS(-5)"},
		{"AbsNull", "SELECT ABS(NULL)"},
		{"RoundPos", "SELECT ROUND(1.5)"},
		{"CeilFloor", "SELECT CEIL(1.1), FLOOR(1.9)"},
		{"Sqrt", "SELECT ROUND(SQRT(4), 1)"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
