package E071

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E07102_L1(t *testing.T) {
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

	sqlvibeDB.Exec("CREATE TABLE orders (id INTEGER, cust_id INTEGER, amount INTEGER)")
	sqliteDB.Exec("CREATE TABLE orders (id INTEGER, cust_id INTEGER, amount INTEGER)")
	sqlvibeDB.Exec("CREATE TABLE customers (id INTEGER, name TEXT)")
	sqliteDB.Exec("CREATE TABLE customers (id INTEGER, name TEXT)")

	sqlvibeDB.Exec("INSERT INTO customers VALUES (1, 'alice')")
	sqliteDB.Exec("INSERT INTO customers VALUES (1, 'alice')")
	sqlvibeDB.Exec("INSERT INTO customers VALUES (2, 'bob')")
	sqliteDB.Exec("INSERT INTO customers VALUES (2, 'bob')")
	sqlvibeDB.Exec("INSERT INTO orders VALUES (1, 1, 100)")
	sqliteDB.Exec("INSERT INTO orders VALUES (1, 1, 100)")
	sqlvibeDB.Exec("INSERT INTO orders VALUES (2, 2, 200)")
	sqliteDB.Exec("INSERT INTO orders VALUES (2, 2, 200)")
	sqlvibeDB.Exec("INSERT INTO orders VALUES (3, 1, 50)")
	sqliteDB.Exec("INSERT INTO orders VALUES (3, 1, 50)")

	tests := []struct {
		name string
		sql  string
	}{
		{"SingleTable", "SELECT id FROM customers ORDER BY id"},
		{"FromSubquery", "SELECT * FROM (SELECT id, name FROM customers) AS sub ORDER BY id"},
		{"FromSubqueryWithFilter", "SELECT * FROM (SELECT id, name FROM customers WHERE id > 1) AS sub ORDER BY id"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
