package W014

import (
	"database/sql"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/TS/SQL1999"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_W014_LagLead_L1(t *testing.T) {
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

	for _, stmt := range []string{
		"CREATE TABLE sales (id INTEGER, month INTEGER, amount INTEGER, dept TEXT)",
		"INSERT INTO sales VALUES (1, 1, 100, 'Eng')",
		"INSERT INTO sales VALUES (2, 2, 150, 'Eng')",
		"INSERT INTO sales VALUES (3, 3, 120, 'Eng')",
		"INSERT INTO sales VALUES (4, 4, 200, 'Eng')",
		"INSERT INTO sales VALUES (5, 1, 80, 'HR')",
		"INSERT INTO sales VALUES (6, 2, 90, 'HR')",
		"INSERT INTO sales VALUES (7, 3, 110, 'HR')",
		"INSERT INTO sales VALUES (8, 4, 95, 'HR')",
	} {
		sqlvibeDB.Exec(stmt)
		sqliteDB.Exec(stmt)
	}

	tests := []struct{ name, sql string }{
		{
			"LagBasicPreviousRow",
			"SELECT id, month, amount, LAG(amount) OVER (ORDER BY month) AS prev_amount FROM sales WHERE dept = 'Eng' ORDER BY month",
		},
		{
			"LeadBasicNextRow",
			"SELECT id, month, amount, LEAD(amount) OVER (ORDER BY month) AS next_amount FROM sales WHERE dept = 'Eng' ORDER BY month",
		},
		{
			"LagOffset2",
			"SELECT id, month, amount, LAG(amount, 2) OVER (ORDER BY month) AS prev2_amount FROM sales WHERE dept = 'Eng' ORDER BY month",
		},
		{
			"LeadOffset2",
			"SELECT id, month, amount, LEAD(amount, 2) OVER (ORDER BY month) AS next2_amount FROM sales WHERE dept = 'Eng' ORDER BY month",
		},
		{
			"LagPartitionByDept",
			"SELECT id, dept, month, amount, LAG(amount) OVER (PARTITION BY dept ORDER BY month) AS prev_amount FROM sales ORDER BY dept, month",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
