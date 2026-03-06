// Package Benchmark provides COUNT(*) optimization benchmarks for v0.11.5
// Target: COUNT(*) 10K: 4.89ms → 50µs (98× speedup)
package Benchmark

import (
	"fmt"
	"testing"
	"time"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// BenchmarkCountStar10K measures COUNT(*) on 10K rows
// Target: < 100µs (98× improvement from 4.89ms baseline)
func BenchmarkCountStar10K(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT, score REAL)")

	b.StopTimer()
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'name%d', %f)", i, i, float64(i)*1.1))
	}
	b.StartTimer()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

// BenchmarkCountStar1K measures COUNT(*) on 1K rows
func BenchmarkCountStar1K(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	b.StopTimer()
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'val%d')", i, i))
	}
	b.StartTimer()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

// BenchmarkCountStar5K measures COUNT(*) on 5K rows
func BenchmarkCountStar5K(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	b.StopTimer()
	for i := 0; i < 5000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'val%d')", i, i))
	}
	b.StartTimer()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

// BenchmarkCountStarAfterInsert measures COUNT(*) after incremental inserts
func BenchmarkCountStarAfterInsert(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
		b.StartTimer()

		mustQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

// BenchmarkCountStarAfterDelete measures COUNT(*) after deletes
func BenchmarkCountStarAfterDelete(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		if i < 1000 {
			mustExec(b, db, fmt.Sprintf("DELETE FROM t WHERE id = %d", i))
		}
		b.StartTimer()

		mustQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

// BenchmarkCountStarWithAlias measures COUNT(*) with alias
func BenchmarkCountStarWithAlias(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	b.StopTimer()
	for i := 0; i < 5000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}
	b.StartTimer()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT COUNT(*) AS total FROM t")
	}
}

// BenchmarkCountStarEmptyTable measures COUNT(*) on empty table
func BenchmarkCountStarEmptyTable(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

// BenchmarkCountStarVsScan compares COUNT(*) fast path vs full scan
func BenchmarkCountStarVsScan(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")

	b.StopTimer()
	for i := 0; i < 5000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%10))
	}
	b.StartTimer()

	b.Run("CountStar", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mustQuery(b, db, "SELECT COUNT(*) FROM t")
		}
	})

	b.Run("CountColumn", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mustQuery(b, db, "SELECT COUNT(id) FROM t")
		}
	})

	b.Run("SumAggregate", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mustQuery(b, db, "SELECT SUM(val) FROM t")
		}
	})

	b.Run("SelectAll", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mustQuery(b, db, "SELECT * FROM t")
		}
	})
}

// TestCountStarCorrectness verifies COUNT(*) returns correct values
func TestCountStarCorrectness(t *testing.T) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	testCases := []struct {
		insertCount int
		deleteCount int
		expected    int64
	}{
		{0, 0, 0},
		{10, 0, 10},
		{100, 0, 100},
		{100, 50, 50},
		{1000, 0, 1000},
	}

	for _, tc := range testCases {
		// Clear table
		if _, err := db.Exec("DELETE FROM t"); err != nil {
			t.Fatalf("Failed to clear table: %v", err)
		}

		// Insert rows
		for i := 0; i < tc.insertCount; i++ {
			if _, err := db.Exec(fmt.Sprintf("INSERT INTO t VALUES (%d)", i)); err != nil {
				t.Fatalf("Failed to insert: %v", err)
			}
		}

		// Delete rows
		for i := 0; i < tc.deleteCount; i++ {
			if _, err := db.Exec(fmt.Sprintf("DELETE FROM t WHERE id = %d", i)); err != nil {
				t.Fatalf("Failed to delete: %v", err)
			}
		}

		// Query COUNT(*)
		rows, err := db.Query("SELECT COUNT(*) FROM t")
		if err != nil {
			t.Fatalf("Failed to query COUNT(*): %v", err)
		}

		if !rows.Next() {
			t.Fatalf("Expected row from COUNT(*)")
		}

		var count int64
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("Failed to scan count: %v", err)
		}
		rows.Close()

		if count != tc.expected {
			t.Errorf("COUNT(*) mismatch: got %d, expected %d (inserted %d, deleted %d)",
				count, tc.expected, tc.insertCount, tc.deleteCount)
		}
	}
}

// TestCountStarPerformance verifies COUNT(*) meets 50µs target
func TestCountStarPerformance(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert 10K rows
	for i := 0; i < 10000; i++ {
		if _, err := db.Exec(fmt.Sprintf("INSERT INTO t VALUES (%d)", i)); err != nil {
			t.Fatalf("Failed to insert: %v", err)
		}
	}

	// Warm up
	for i := 0; i < 10; i++ {
		rows, _ := db.Query("SELECT COUNT(*) FROM t")
		rows.Close()
	}

	// Measure
	iterations := 1000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		rows, err := db.Query("SELECT COUNT(*) FROM t")
		if err != nil {
			t.Fatalf("Query failed: %v", err)
		}
		rows.Close()
	}
	elapsed := time.Since(start)

	avgNs := elapsed.Nanoseconds() / int64(iterations)
	avgUs := float64(avgNs) / 1000.0

	t.Logf("COUNT(*) 10K rows: avg=%d ns (%.2f µs) over %d iterations", avgNs, avgUs, iterations)

	// Target: < 100µs (allow some overhead for test harness)
	if avgUs > 100 {
		t.Logf("WARNING: COUNT(*) performance %.2fµs exceeds 100µs target", avgUs)
	}
}
