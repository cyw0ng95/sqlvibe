package F651

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F651_Printf_L1 tests the printf() / format() function.
func TestSQL1999_F651_Printf_L1(t *testing.T) {
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
		{"PrintfHello", "SELECT printf('Hello %s!', 'World')"},
		{"PrintfInt", "SELECT printf('%d', 42)"},
		{"PrintfFloat", "SELECT printf('%.2f', 3.14159)"},
		{"PrintfPad", "SELECT printf('%05d', 7)"},
		{"PrintfMulti", "SELECT printf('%s=%d', 'x', 99)"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F651_Quote_L1 tests the quote() function.
func TestSQL1999_F651_Quote_L1(t *testing.T) {
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
		{"QuoteSimple", "SELECT quote('hello')"},
		{"QuoteApostrophe", "SELECT quote('it''s a test')"},
		{"QuoteNull", "SELECT quote(NULL)"},
		{"QuoteInt", "SELECT quote(42)"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F651_Instr_L1 tests INSTR function.
func TestSQL1999_F651_Instr_L1(t *testing.T) {
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
		{"InstrFound", "SELECT instr('hello world', 'world')"},
		{"InstrNotFound", "SELECT instr('hello', 'xyz')"},
		{"InstrFirst", "SELECT instr('abcabc', 'b')"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F651_HexChar_L1 tests hex() and char() functions.
func TestSQL1999_F651_HexChar_L1(t *testing.T) {
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
		{"HexStr", "SELECT hex('ABC')"},
		{"CharCode", "SELECT char(65, 66, 67)"},
		{"Unicode", "SELECT unicode('A')"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
