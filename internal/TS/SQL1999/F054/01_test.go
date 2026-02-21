package F054

import (
	"database/sql"
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/TS/SQL1999"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

func TestSQL1999_F054_CountDistinct_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE items (id INTEGER, category TEXT, price INTEGER)"},
		{"Insert1", "INSERT INTO items VALUES (1, 'A', 10)"},
		{"Insert2", "INSERT INTO items VALUES (2, 'B', 20)"},
		{"Insert3", "INSERT INTO items VALUES (3, 'A', 30)"},
		{"Insert4", "INSERT INTO items VALUES (4, 'C', 10)"},
		{"Insert5", "INSERT INTO items VALUES (5, 'B', 20)"},
		{"Insert6", "INSERT INTO items VALUES (6, 'A', 10)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"CountDistinctCategory", "SELECT COUNT(DISTINCT category) FROM items"},
		{"CountDistinctPrice", "SELECT COUNT(DISTINCT price) FROM items"},
		{"CountAll", "SELECT COUNT(*) FROM items"},
		{"CountDistinctVsAll", "SELECT COUNT(*), COUNT(DISTINCT category) FROM items"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F054_SumAvgDistinct_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE nums (id INTEGER, val INTEGER)"},
		{"Insert1", "INSERT INTO nums VALUES (1, 5)"},
		{"Insert2", "INSERT INTO nums VALUES (2, 10)"},
		{"Insert3", "INSERT INTO nums VALUES (3, 5)"},
		{"Insert4", "INSERT INTO nums VALUES (4, 20)"},
		{"Insert5", "INSERT INTO nums VALUES (5, 10)"},
		{"Insert6", "INSERT INTO nums VALUES (6, 5)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SumDistinct", "SELECT SUM(DISTINCT val) FROM nums"},
		{"SumAll", "SELECT SUM(val) FROM nums"},
		{"AvgDistinct", "SELECT AVG(DISTINCT val) FROM nums"},
		{"AvgAll", "SELECT AVG(val) FROM nums"},
		{"MinMax", "SELECT MIN(val), MAX(val) FROM nums"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F054_GroupConcatBasic_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE tags (id INTEGER, tag TEXT)"},
		{"Insert1", "INSERT INTO tags VALUES (1, 'alpha')"},
		{"Insert2", "INSERT INTO tags VALUES (2, 'beta')"},
		{"Insert3", "INSERT INTO tags VALUES (3, 'gamma')"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	// Only test that GROUP_CONCAT executes without error using CompareExecResults
	execTests := []struct {
		name string
		sql  string
	}{
		{"GroupConcatExec", "SELECT GROUP_CONCAT(tag) FROM tags"},
	}

	for _, tt := range execTests {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F054_AggregatesWithGroupBy_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE sales (id INTEGER, dept TEXT, amount INTEGER)"},
		{"Insert1", "INSERT INTO sales VALUES (1, 'A', 100)"},
		{"Insert2", "INSERT INTO sales VALUES (2, 'A', 200)"},
		{"Insert3", "INSERT INTO sales VALUES (3, 'B', 150)"},
		{"Insert4", "INSERT INTO sales VALUES (4, 'B', 150)"},
		{"Insert5", "INSERT INTO sales VALUES (5, 'C', 300)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"SumByDept", "SELECT dept, SUM(amount) FROM sales GROUP BY dept ORDER BY dept"},
		{"CountByDept", "SELECT dept, COUNT(*) FROM sales GROUP BY dept ORDER BY dept"},
		{"AvgByDept", "SELECT dept, AVG(amount) FROM sales GROUP BY dept ORDER BY dept"},
		{"SumDistinctByDept", "SELECT dept, SUM(DISTINCT amount) FROM sales GROUP BY dept ORDER BY dept"},
		{"CountDistinctByDept", "SELECT dept, COUNT(DISTINCT amount) FROM sales GROUP BY dept ORDER BY dept"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}

func TestSQL1999_F054_AggregatesEmptyTable_L1(t *testing.T) {
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

	setup := []struct {
		name string
		sql  string
	}{
		{"CreateTable", "CREATE TABLE empty_t (id INTEGER, val INTEGER)"},
	}

	for _, tt := range setup {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareExecResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}

	queries := []struct {
		name string
		sql  string
	}{
		{"CountEmpty", "SELECT COUNT(*) FROM empty_t"},
		{"SumEmpty", "SELECT SUM(val) FROM empty_t"},
		{"AvgEmpty", "SELECT AVG(val) FROM empty_t"},
		{"MinEmpty", "SELECT MIN(val) FROM empty_t"},
		{"MaxEmpty", "SELECT MAX(val) FROM empty_t"},
	}

	for _, tt := range queries {
		t.Run(tt.name, func(t *testing.T) {
			SQL1999.CompareQueryResults(t, sqlvibeDB, sqliteDB, tt.sql, tt.name)
		})
	}
}
