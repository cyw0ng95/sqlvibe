package F031

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_F03103_L1(t *testing.T) {
	t.Skip("GRANT/REVOKE not supported by SQLite - test exists for SQL1999 conformance documentation")
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
		{"GrantSelect", "GRANT SELECT ON t1 TO test_user"},
		{"GrantInsert", "GRANT INSERT ON t1 TO test_user"},
		{"GrantUpdate", "GRANT UPDATE ON t1 TO test_user"},
		{"GrantDelete", "GRANT DELETE ON t1 TO test_user"},
		{"GrantAll", "GRANT ALL PRIVILEGES ON t1 TO test_user"},
		{"RevokeSelect", "REVOKE SELECT ON t1 FROM test_user"},
		{"RevokeAll", "REVOKE ALL PRIVILEGES ON t1 FROM test_user"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
