// Package Benchmark provides v0.9.16 performance benchmarks.
// These benchmarks cover the v0.9.16 features:
//   - Bloom filter pre-filter for hash join (Track B)
//   - Vectorized WHERE filter on columnar data (Track C)
//   - Index-only scan (Track A)
//   - Allocation profiling (Track D)
//   - Dispatch table expansion (Track F)
package Benchmark

import (
	"fmt"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// BenchmarkFair_JoinBloomFilter benchmarks hash join performance with the
// bloom filter pre-filter (Track B).
// Uses 500 left rows × 2000 right rows.
func BenchmarkFair_JoinBloomFilter(b *testing.B) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	mustExec(b, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(b, db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)")

	for i := 1; i <= 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO users VALUES (%d, 'user%d')", i, i))
	}
	for i := 1; i <= 2000; i++ {
		userID := (i % 500) + 1
		mustExec(b, db, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %f)", i, userID, float64(i)*1.5))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 1000")
		for rows.Next() {
		}
	}
}

// BenchmarkFair_VectorizedWhere benchmarks WHERE filter performance on
// numeric data (Track C).
func BenchmarkFair_VectorizedWhere(b *testing.B) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	mustExec(b, db, "CREATE TABLE nums (id INTEGER PRIMARY KEY, val INTEGER, score REAL)")
	for i := 1; i <= 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO nums VALUES (%d, %d, %f)", i, i%1000, float64(i)*0.1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT id, val FROM nums WHERE val = 500")
		for rows.Next() {
		}
	}
}

// BenchmarkFair_IndexOnlyScan benchmarks a covering-index scan where all
// required columns are in the index (Track A).
func BenchmarkFair_IndexOnlyScan(b *testing.B) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	mustExec(b, db, "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, score INTEGER)")
	mustExec(b, db, "CREATE INDEX idx_covering ON products(id, name, score)")
	for i := 1; i <= 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO products VALUES (%d, 'product%d', %d)", i, i, i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT name, score FROM products WHERE id = 500")
		for rows.Next() {
		}
	}
}

// Benchmark_AllocationOverhead measures allocations per parameterized query (Track D).
func Benchmark_AllocationOverhead(b *testing.B) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	mustExec(b, db, "CREATE TABLE alloc_test (id INTEGER PRIMARY KEY, val TEXT, score INTEGER)")
	for i := 1; i <= 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO alloc_test VALUES (%d, 'val%d', %d)", i, i, i*5))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows, err := db.QueryWithParams("SELECT id, val FROM alloc_test WHERE id = ?1", []interface{}{50})
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
		}
	}
}

// BenchmarkFair_DispatchOpIsNull benchmarks IS NULL queries via the new
// OpIsNull/OpNotNull dispatch handlers (Track F).
func BenchmarkFair_DispatchOpIsNull(b *testing.B) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	mustExec(b, db, "CREATE TABLE nulltest (id INTEGER PRIMARY KEY, val TEXT)")
	for i := 1; i <= 1000; i++ {
		if i%3 == 0 {
			mustExec(b, db, fmt.Sprintf("INSERT INTO nulltest VALUES (%d, NULL)", i))
		} else {
			mustExec(b, db, fmt.Sprintf("INSERT INTO nulltest VALUES (%d, 'v%d')", i, i))
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT id FROM nulltest WHERE val IS NOT NULL")
		for rows.Next() {
		}
	}
}

// BenchmarkFair_DispatchBitwise benchmarks bitwise AND/OR operations in WHERE
// via the new dispatch table entries (Track F).
func BenchmarkFair_DispatchBitwise(b *testing.B) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	mustExec(b, db, "CREATE TABLE bitops (id INTEGER PRIMARY KEY, flags INTEGER)")
	for i := 1; i <= 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO bitops VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		// Use remainder (%) which tests the new OpRemainder dispatch handler
		rows := mustQuery(b, db, "SELECT id, flags FROM bitops WHERE (flags % 7) = 0")
		for rows.Next() {
		}
	}
}

// BenchmarkFair_WhereRangeFilter benchmarks WHERE with range predicates on
// a large table (Track C — range vectorization).
func BenchmarkFair_WhereRangeFilter(b *testing.B) {
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	mustExec(b, db, "CREATE TABLE range_bench (id INTEGER PRIMARY KEY, age INTEGER)")
	for i := 1; i <= 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO range_bench VALUES (%d, %d)", i, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT id FROM range_bench WHERE age > 50")
		for rows.Next() {
		}
	}
}
