package Q051

import (
	"database/sql"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
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

func createSalesTable(sv *sqlvibe.Database, sl *sql.DB) {
	sv.Exec("CREATE TABLE sales (id INTEGER, region TEXT, product TEXT, amount INTEGER)")
	sl.Exec("CREATE TABLE sales (id INTEGER, region TEXT, product TEXT, amount INTEGER)")

	rows := [][4]string{
		{"1", "'north'", "'widget'", "100"},
		{"2", "'north'", "'gadget'", "200"},
		{"3", "'south'", "'widget'", "150"},
		{"4", "'south'", "'gadget'", "300"},
		{"5", "'north'", "'widget'", "120"},
		{"6", "'south'", "'gadget'", "250"},
	}
	for _, r := range rows {
		q := "INSERT INTO sales VALUES (" + r[0] + ", " + r[1] + ", " + r[2] + ", " + r[3] + ")"
		sv.Exec(q)
		sl.Exec(q)
	}
}

// TestSQL1999_Q051_GroupBySingle_L1 tests GROUP BY with a single column.
func TestSQL1999_Q051_GroupBySingle_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createSalesTable(sv, sl)

	tests := []struct{ name, sql string }{
		{"GroupByRegion", "SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY region"},
		{"GroupByProduct", "SELECT product, SUM(amount) FROM sales GROUP BY product ORDER BY product"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q051_GroupByMultiple_L1 tests GROUP BY with multiple columns.
func TestSQL1999_Q051_GroupByMultiple_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createSalesTable(sv, sl)

	tests := []struct{ name, sql string }{
		{"GroupByRegionProduct", "SELECT region, product, SUM(amount) FROM sales GROUP BY region, product ORDER BY region, product"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q051_Having_L1 tests HAVING clause.
func TestSQL1999_Q051_Having_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createSalesTable(sv, sl)

	tests := []struct{ name, sql string }{
		{"HavingCount", "SELECT region, COUNT(*) AS cnt FROM sales GROUP BY region HAVING COUNT(*) > 2 ORDER BY region"},
		{"HavingSum", "SELECT product, SUM(amount) AS total FROM sales GROUP BY product HAVING SUM(amount) > 300 ORDER BY product"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q051_GroupByAggregates_L1 tests COUNT, SUM, AVG with GROUP BY.
func TestSQL1999_Q051_GroupByAggregates_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createSalesTable(sv, sl)

	tests := []struct{ name, sql string }{
		{"GroupByCount", "SELECT region, COUNT(*) FROM sales GROUP BY region ORDER BY region"},
		{"GroupBySum", "SELECT region, SUM(amount) FROM sales GROUP BY region ORDER BY region"},
		{"GroupByAvg", "SELECT region, AVG(amount) FROM sales GROUP BY region ORDER BY region"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}

// TestSQL1999_Q051_GroupByOrderBy_L1 tests GROUP BY combined with ORDER BY.
func TestSQL1999_Q051_GroupByOrderBy_L1(t *testing.T) {
	sv, sl := setup(t)
	defer sv.Close()
	defer sl.Close()
	createSalesTable(sv, sl)

	tests := []struct{ name, sql string }{
		{"GroupByOrderByAsc", "SELECT product, SUM(amount) AS total FROM sales GROUP BY product ORDER BY total ASC"},
		{"GroupByOrderByDesc", "SELECT product, SUM(amount) AS total FROM sales GROUP BY product ORDER BY total DESC"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sv, sl, tt.sql, tt.name)
		})
	}
}
