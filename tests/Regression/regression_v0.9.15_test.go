package Regression

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// TestRegression_DistinctWithOrderByExtraCol_L1 tests SELECT DISTINCT with ORDER BY
// referencing columns NOT in the SELECT list.
// Bug: Extra ORDER BY columns were added to each row for sorting, causing deduplicateRows
// to use those extra columns as part of the dedup key, so rows that shared the same
// projected SELECT column but had different ORDER BY column values were not deduplicated.
// Fixed in SQLValidator run (2026-02-25) by deferring DISTINCT to after sort+strip.
func TestRegression_DistinctWithOrderByExtraCol_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.MustExec("CREATE TABLE t (id INTEGER PRIMARY KEY, grp INTEGER NOT NULL, val INTEGER NOT NULL)")
	db.MustExec("INSERT INTO t VALUES (1, 1, 10)")
	db.MustExec("INSERT INTO t VALUES (2, 1, 20)")
	db.MustExec("INSERT INTO t VALUES (3, 2, 30)")
	db.MustExec("INSERT INTO t VALUES (4, 2, 40)")
	db.MustExec("INSERT INTO t VALUES (5, 3, 50)")

	// ORDER BY uses 'val' which is NOT in the SELECT DISTINCT list.
	// DISTINCT should produce {1,2,3} regardless of ORDER BY column ordering.
	rows, err := db.Query("SELECT DISTINCT grp FROM t ORDER BY val ASC")
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if len(rows.Data) != 3 {
		t.Fatalf("expected 3 distinct grp values, got %d: %v", len(rows.Data), rows.Data)
	}
	// After ORDER BY val ASC + DISTINCT on grp, SQLite returns [1,2,3]
	got := make([]int64, len(rows.Data))
	for i, row := range rows.Data {
		got[i] = row[0].(int64)
	}
	if got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Errorf("expected [1,2,3], got %v", got)
	}
}

// TestRegression_DistinctOrderByExtraColWithLimit_L1 tests DISTINCT + ORDER BY extra
// non-SELECT cols + LIMIT. The LIMIT must apply to the deduplicated result, not the
// pre-dedup sorted stream.
func TestRegression_DistinctOrderByExtraColWithLimit_L1(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	db.MustExec("CREATE TABLE ol (o_id INTEGER, d_id INTEGER, w_id INTEGER, number INTEGER)")
	db.MustExec("INSERT INTO ol VALUES (1,1,1,1)")
	db.MustExec("INSERT INTO ol VALUES (1,1,1,2)")
	db.MustExec("INSERT INTO ol VALUES (1,1,1,3)")
	db.MustExec("INSERT INTO ol VALUES (2,2,1,1)")
	db.MustExec("INSERT INTO ol VALUES (2,2,1,2)")

	// ORDER BY contains o_id, d_id, w_id, number – none of which are in SELECT.
	// DISTINCT on d_id should give 2 unique values {1, 2}.
	// LIMIT 4 should be applied AFTER deduplication, so we get 2 rows (not 4).
	rows, err := db.Query("SELECT DISTINCT d_id FROM ol ORDER BY o_id ASC, d_id ASC, w_id ASC, number ASC LIMIT 4")
	if err != nil {
		t.Fatalf("query: %v", err)
	}

	if len(rows.Data) != 2 {
		t.Fatalf("expected 2 distinct d_id values, got %d: %v", len(rows.Data), rows.Data)
	}
	if rows.Data[0][0].(int64) != 1 || rows.Data[1][0].(int64) != 2 {
		t.Errorf("expected [1, 2], got %v and %v", rows.Data[0], rows.Data[1])
	}
}
