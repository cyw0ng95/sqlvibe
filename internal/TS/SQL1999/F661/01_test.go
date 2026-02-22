package F661

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestSQL1999_F661_PragmaEncoding_L1 tests PRAGMA encoding.
func TestSQL1999_F661_PragmaEncoding_L1(t *testing.T) {
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

	SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, "PRAGMA encoding", "PragmaEncoding")
}

// TestSQL1999_F661_PragmaCollationList_L1 tests PRAGMA collation_list.
func TestSQL1999_F661_PragmaCollationList_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer db.Close()

	rows, err := db.Query("PRAGMA collation_list")
	if err != nil {
		t.Fatalf("PRAGMA collation_list error: %v", err)
	}
	if len(rows.Data) == 0 {
		t.Error("Expected at least 1 collation")
	}
	// Should contain BINARY
	found := false
	for _, row := range rows.Data {
		if row[1] == "BINARY" {
			found = true
		}
	}
	if !found {
		t.Error("BINARY collation not found in collation_list")
	}
}

// TestSQL1999_F661_PragmaForeignKeyToggle_L1 tests toggling PRAGMA foreign_keys.
func TestSQL1999_F661_PragmaForeignKeyToggle_L1(t *testing.T) {
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

	tests := []struct {
		name string
		sql  string
	}{
		{"FK_Default", "PRAGMA foreign_keys"},
		{"FK_Enable", "PRAGMA foreign_keys = ON"},
		{"FK_Check_On", "PRAGMA foreign_keys"},
		{"FK_Disable", "PRAGMA foreign_keys = OFF"},
		{"FK_Check_Off", "PRAGMA foreign_keys"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
