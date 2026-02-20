package E041

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F301_E04104_L1(t *testing.T) {
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
		{"CreateOrdersBasicFK", "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id))"},
		{"FKWithOnDeleteCascade", "CREATE TABLE orders_cascade (id INTEGER PRIMARY KEY, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE)"},
		{"FKWithOnDeleteRestrict", "CREATE TABLE orders_restrict (id INTEGER PRIMARY KEY, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE RESTRICT)"},
		{"FKWithOnDeleteSetNull", "CREATE TABLE orders_setnull (id INTEGER PRIMARY KEY, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL)"},
		{"FKWithOnUpdateCascade", "CREATE TABLE orders_update (id INTEGER PRIMARY KEY, user_id INTEGER, FOREIGN KEY (user_id) REFERENCES users(id) ON UPDATE CASCADE)"},
		{"MultipleFKs", "CREATE TABLE items (id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, FOREIGN KEY (order_id) REFERENCES orders(id), FOREIGN KEY (product_id) REFERENCES products(id))"},
		{"CreateProducts", "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)"},
	}

	for _, tt := range createTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	insertTests := []struct {
		name string
		sql  string
	}{
		{"InsertUsers", "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"},
		{"InsertOrders", "INSERT INTO orders VALUES (1, 1), (2, 2)"},
		{"InsertOrdersCascade", "INSERT INTO orders_cascade VALUES (1, 1), (2, 2)"},
		{"InsertOrdersRestrict", "INSERT INTO orders_restrict VALUES (1, 1), (2, 2)"},
		{"InsertProducts", "INSERT INTO products VALUES (1, 'Product A', 10.99), (2, 'Product B', 20.99)"},
	}

	for _, tt := range insertTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queryTests := []struct {
		name string
		sql  string
	}{
		{"SelectFromUsers", "SELECT * FROM users ORDER BY id"},
		{"SelectFromOrders", "SELECT * FROM orders ORDER BY id"},
		{"JoinOrdersUsers", "SELECT o.*, u.name FROM orders o JOIN users u ON o.user_id = u.id"},
		{"SelectFromProducts", "SELECT * FROM products ORDER BY id"},
	}

	for _, tt := range queryTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
