package F241

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F24101_L1(t *testing.T) {
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

	tests := []struct {
		name string
		sql  string
	}{
		{"RowConstructor2", "SELECT (1, 2)"},
		{"RowConstructor3", "SELECT ROW(1, 2)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// VALUES table constructor: sqlvibe supports it, older SQLite does not
	t.Run("RowConstructor", func(t *testing.T) {
		rows := SQL1999.QuerySqlvibeOnly(t, sqlvibeDB,
			"SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(a, b) ORDER BY a", "RowConstructor")
		if rows == nil {
			return
		}
		if len(rows.Data) != 2 {
			t.Errorf("RowConstructor: got %d rows, want 2", len(rows.Data))
		}
	})
}
