package Q061

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
	_ "github.com/glebarez/go-sqlite"
)

func setup(t *testing.T) (*sqlvibe.Database, *sql.DB) {
	t.Helper()
	sv, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("open sqlvibe: %v", err)
	}
	sl, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return sv, sl
}

func createOrderTable(sv *sqlvibe.Database, sl *sql.DB) {
	sv.Exec("CREATE TABLE items (id INTEGER, category TEXT, price INTEGER, name TEXT)")
	sl.Exec("CREATE TABLE items (id INTEGER, category TEXT, price INTEGER, name TEXT)")

	rows := [][4]string{
		{"1", "'A'", "30", "'alpha'"},
		{"2", "'B'", "10", "'beta'"},
		{"3", "'A'", "50", "'gamma'"},
		{"4", "'B'", "20", "'delta'"},
		{"5", "'A'", "40", "'epsilon'"},
	}
	for _, r := range rows {
		q := "INSERT INTO items VALUES (" + r[0] + ", " + r[1] + ", " + r[2] + ", " + r[3] + ")"
		sv.Exec(q)
		sl.Exec(q)
	}
}

// TestSQL1999_Q061_OrderByAsc_L1 tests ORDER BY ASC.
func TestSQL1999_Q061_OrderByAsc_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createOrderTable(sv, sl)

	tests := []struct{ name, sql string }{
		{"OrderByPriceAsc", "SELECT id, price FROM items ORDER BY price ASC"},
		{"OrderByNameAsc", "SELECT id, name FROM items ORDER BY name ASC"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q061_OrderByDesc_L1 tests ORDER BY DESC.
func TestSQL1999_Q061_OrderByDesc_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createOrderTable(sv, sl)

	tests := []struct{ name, sql string }{
		{"OrderByPriceDesc", "SELECT id, price FROM items ORDER BY price DESC"},
		{"OrderByIdDesc", "SELECT id, name FROM items ORDER BY id DESC"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q061_OrderByMultiple_L1 tests ORDER BY multiple columns.
func TestSQL1999_Q061_OrderByMultiple_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createOrderTable(sv, sl)

	tests := []struct{ name, sql string }{
		{"OrderByCategoryThenPrice", "SELECT id, category, price FROM items ORDER BY category ASC, price DESC"},
		{"OrderByPriceThenName", "SELECT id, price, name FROM items ORDER BY price ASC, name ASC"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q061_OrderByLimit_L1 tests ORDER BY with LIMIT.
func TestSQL1999_Q061_OrderByLimit_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createOrderTable(sv, sl)

	tests := []struct{ name, sql string }{
		{"OrderByLimit", "SELECT id, price FROM items ORDER BY price DESC LIMIT 3"},
		{"OrderByLimitOne", "SELECT id, price FROM items ORDER BY price ASC LIMIT 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q061_OrderByOffset_L1 tests ORDER BY with OFFSET.
func TestSQL1999_Q061_OrderByOffset_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createOrderTable(sv, sl)

	tests := []struct{ name, sql string }{
		{"OrderByOffset", "SELECT id, price FROM items ORDER BY price ASC LIMIT 2 OFFSET 2"},
		{"OrderByOffsetAll", "SELECT id, price FROM items ORDER BY id ASC LIMIT 10 OFFSET 3"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}
