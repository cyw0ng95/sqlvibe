package E031

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E03106_L1(t *testing.T) {
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
		{"CreateUsers", "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"},
		{"CreateOrders", "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, FOREIGN KEY (user_id) REFERENCES users(id))"},
		{"CreateProducts", "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category_id INTEGER, FOREIGN KEY (category_id) REFERENCES categories(id))"},
		{"CreateCategories", "CREATE TABLE categories (id INTEGER PRIMARY KEY, name TEXT)"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Note: information_schema is not supported by SQLite, so we only test sqlvibe
	queryTests := []struct {
		name string
		sql  string
	}{
		{"QueryAllRefConstraints", "SELECT * FROM information_schema.referential_constraints"},
		{"QueryByTable", "SELECT * FROM information_schema.referential_constraints WHERE table_name = 'orders'"},
		{"QueryFKRelationships", "SELECT * FROM information_schema.referential_constraints WHERE constraint_schema = 'main'"},
		{"QueryBySchema", "SELECT * FROM information_schema.referential_constraints WHERE constraint_schema = 'main' ORDER BY table_name"},
		{"QueryConstraintNames", "SELECT constraint_name, table_name FROM information_schema.referential_constraints WHERE table_schema = 'main'"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			rows := SQL1999.QuerySqlvibeOnly(t, sqlvibeDB, tt.sql, tt.name)
			if rows == nil {
				return
			}
			// Foreign key constraints may be empty, just verify query succeeds
		})
	}
}
