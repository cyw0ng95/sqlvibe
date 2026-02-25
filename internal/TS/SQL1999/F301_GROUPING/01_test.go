package F301_GROUPING

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func openDB(t *testing.T) *sqlvibe.Database {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	return db
}

// TestSQL1999_F301_GroupByMultiCol_L1 tests GROUP BY with multiple columns.
func TestSQL1999_F301_GroupByMultiCol_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE sales (year INTEGER, region TEXT, amount INTEGER)",
		"INSERT INTO sales VALUES (2022, 'east', 100)",
		"INSERT INTO sales VALUES (2022, 'east', 200)",
		"INSERT INTO sales VALUES (2022, 'west', 150)",
		"INSERT INTO sales VALUES (2023, 'east', 300)",
		"INSERT INTO sales VALUES (2023, 'west', 250)",
		"INSERT INTO sales VALUES (2023, 'west', 100)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT year, region, SUM(amount) FROM sales GROUP BY year, region ORDER BY year, region")
	if err != nil {
		t.Fatalf("GROUP BY multi-col: %v", err)
	}
	if len(rows.Data) != 4 {
		t.Errorf("expected 4 groups, got %d: %v", len(rows.Data), rows.Data)
	}
	// First row: 2022, east, 300
	if rows.Data[0][0] != int64(2022) || rows.Data[0][1] != "east" || rows.Data[0][2] != int64(300) {
		t.Errorf("unexpected first row: %v", rows.Data[0])
	}
}

// TestSQL1999_F301_GroupByHaving_L1 tests GROUP BY combined with HAVING.
func TestSQL1999_F301_GroupByHaving_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE orders (category TEXT, amount INTEGER)",
		"INSERT INTO orders VALUES ('A', 10)",
		"INSERT INTO orders VALUES ('A', 20)",
		"INSERT INTO orders VALUES ('B', 5)",
		"INSERT INTO orders VALUES ('C', 100)",
		"INSERT INTO orders VALUES ('C', 200)",
		"INSERT INTO orders VALUES ('C', 300)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	rows, err := db.Query("SELECT category, SUM(amount) AS total FROM orders GROUP BY category HAVING SUM(amount) > 50 ORDER BY category")
	if err != nil {
		t.Fatalf("GROUP BY HAVING: %v", err)
	}
	if len(rows.Data) != 1 {
		t.Errorf("expected 1 group (C), got %d: %v", len(rows.Data), rows.Data)
	}
	if rows.Data[0][0] != "C" || rows.Data[0][1] != int64(600) {
		t.Errorf("expected C/600, got %v", rows.Data[0])
	}
}

// TestSQL1999_F301_GroupByAggregates_L1 tests GROUP BY with multiple aggregate functions.
func TestSQL1999_F301_GroupByAggregates_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE scores (student TEXT, subject TEXT, score INTEGER)",
		"INSERT INTO scores VALUES ('Alice', 'Math', 90)",
		"INSERT INTO scores VALUES ('Alice', 'Science', 85)",
		"INSERT INTO scores VALUES ('Alice', 'History', 78)",
		"INSERT INTO scores VALUES ('Bob', 'Math', 70)",
		"INSERT INTO scores VALUES ('Bob', 'Science', 95)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	rows, err := db.Query(`
		SELECT student, COUNT(*), MIN(score), MAX(score), SUM(score)
		FROM scores
		GROUP BY student
		ORDER BY student`)
	if err != nil {
		t.Fatalf("GROUP BY aggregates: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows.Data))
	}
	// Alice: count=3, min=78, max=90, sum=253
	aliceRow := rows.Data[0]
	if aliceRow[0] != "Alice" || aliceRow[1] != int64(3) || aliceRow[2] != int64(78) || aliceRow[3] != int64(90) || aliceRow[4] != int64(253) {
		t.Errorf("unexpected Alice row: %v", aliceRow)
	}
}

// TestSQL1999_F301_RollupSimulated_L1 tests ROLLUP-like totals via UNION ALL.
func TestSQL1999_F301_RollupSimulated_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE revenue (dept TEXT, amount INTEGER)",
		"INSERT INTO revenue VALUES ('eng', 500)",
		"INSERT INTO revenue VALUES ('eng', 300)",
		"INSERT INTO revenue VALUES ('mkt', 200)",
		"INSERT INTO revenue VALUES ('mkt', 400)",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Simulate ROLLUP: per-dept totals UNION ALL grand total
	rows, err := db.Query(`
		SELECT dept, SUM(amount) AS total FROM revenue GROUP BY dept
		UNION ALL
		SELECT 'ALL', SUM(amount) FROM revenue
		ORDER BY dept`)
	if err != nil {
		t.Fatalf("ROLLUP via UNION ALL: %v", err)
	}
	if len(rows.Data) != 3 {
		t.Errorf("expected 3 rows, got %d: %v", len(rows.Data), rows.Data)
	}
}

// TestSQL1999_F301_GroupByCount_L1 tests COUNT with GROUP BY and HAVING COUNT.
func TestSQL1999_F301_GroupByCount_L1(t *testing.T) {
	db := openDB(t)
	defer db.Close()

	for _, sql := range []string{
		"CREATE TABLE events (user_id INTEGER, event TEXT)",
		"INSERT INTO events VALUES (1, 'click')",
		"INSERT INTO events VALUES (1, 'click')",
		"INSERT INTO events VALUES (1, 'view')",
		"INSERT INTO events VALUES (2, 'click')",
		"INSERT INTO events VALUES (3, 'view')",
		"INSERT INTO events VALUES (3, 'view')",
		"INSERT INTO events VALUES (3, 'view')",
	} {
		if _, err := db.Exec(sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}

	// Users with more than 2 events
	rows, err := db.Query("SELECT user_id, COUNT(*) FROM events GROUP BY user_id HAVING COUNT(*) > 2 ORDER BY user_id")
	if err != nil {
		t.Fatalf("HAVING COUNT: %v", err)
	}
	if len(rows.Data) != 2 {
		t.Errorf("expected 2 users (user 1 with 3 and user 3 with 3), got %d: %v", len(rows.Data), rows.Data)
	}
}
