package G013

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_G013_G01301_L1(t *testing.T) {
	sqlvibePath := ":memory:"

	sqlvibeDB, err := sqlvibe.Open(sqlvibePath)
	if err != nil {
		t.Fatalf("Failed to open sqlvibe: %v", err)
	}
	defer sqlvibeDB.Close()

	// Create some tables so information_schema has content
	sqlvibeDB.Exec("CREATE TABLE employees (id INTEGER PRIMARY KEY, name TEXT, dept TEXT)")
	sqlvibeDB.Exec("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)")
	sqlvibeDB.Exec("CREATE TABLE projects (id INTEGER PRIMARY KEY, title TEXT, budget REAL)")

	// Note: information_schema is not supported by standard SQLite, so we only test sqlvibe
	queryTests := []struct {
		name    string
		sql     string
		wantMin int
	}{
		{"QueryAllTables", "SELECT * FROM information_schema.tables", 1},
		{"QueryMainSchema", "SELECT * FROM information_schema.tables WHERE table_schema = 'main'", 1},
		{"QueryTableNames", "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'", 1},
		{"QueryTableTypes", "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'main'", 1},
		{"QueryColumns", "SELECT * FROM information_schema.columns", 1},
		{"QueryColumnsByTable", "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'employees'", 1},
	}

	for _, tt := range queryTests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			rows := SQL1999.QuerySqlvibeOnly(t, sqlvibeDB, tt.sql, tt.name)
			if rows == nil {
				return
			}
			if len(rows.Data) < tt.wantMin {
				t.Errorf("%s: expected at least %d rows, got %d", tt.name, tt.wantMin, len(rows.Data))
			}
		})
	}
}
