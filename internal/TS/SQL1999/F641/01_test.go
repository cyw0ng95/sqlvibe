package F641

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F641_JulianDay_L1 tests julianday() function.
func TestSQL1999_F641_JulianDay_L1(t *testing.T) {
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
		{"JulianNow", "SELECT ROUND(julianday('now'), 0) > 2400000"},
		{"JulianFixed", "SELECT ROUND(julianday('2000-01-01'), 5)"},
		{"JulianDiff", "SELECT ROUND(julianday('2024-01-01') - julianday('2023-01-01'))"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F641_Strftime_L1 tests strftime() with extended format specifiers.
func TestSQL1999_F641_Strftime_L1(t *testing.T) {
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
		{"StrftimeYear", "SELECT strftime('%Y', '2024-06-15 10:30:00')"},
		{"StrftimeMonth", "SELECT strftime('%m', '2024-06-15 10:30:00')"},
		{"StrftimeDay", "SELECT strftime('%d', '2024-06-15 10:30:00')"},
		{"StrftimeHour", "SELECT strftime('%H', '2024-06-15 10:30:00')"},
		{"StrftimeMin", "SELECT strftime('%M', '2024-06-15 10:30:00')"},
		{"StrftimeSec", "SELECT strftime('%S', '2024-06-15 10:30:00')"},
		{"StrftimeWeekday", "SELECT strftime('%w', '2024-01-01')"},
		{"StrftimeAll", "SELECT strftime('%Y-%m-%d %H:%M:%S', '2024-06-15 10:30:45')"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_F641_UnixEpoch_L1 tests unixepoch() function.
func TestSQL1999_F641_UnixEpoch_L1(t *testing.T) {
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
		{"UnixEpoch1970", "SELECT unixepoch('1970-01-01 00:00:00')"},
		{"UnixEpoch2000", "SELECT unixepoch('2000-01-01 00:00:00')"},
	}
	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
