// Package Benchmark provides v0.8.3 performance benchmarks.
// These benchmarks focus on batch INSERT throughput and allocation reduction.
package Benchmark

import (
	"fmt"
	"strings"
	"testing"
)

// -----------------------------------------------------------------
// Phase 3: Batch INSERT benchmarks
// -----------------------------------------------------------------

// BenchmarkBatchInsert_10 benchmarks inserting 10 rows in a single INSERT.
func BenchmarkBatchInsert_10(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")

	// Build a 10-row INSERT.
	var sb strings.Builder
	for i := 0; i < b.N; i++ {
		sb.Reset()
		sb.WriteString("INSERT INTO t VALUES ")
		for j := 0; j < 10; j++ {
			if j > 0 {
				sb.WriteString(", ")
			}
			fmt.Fprintf(&sb, "(%d, %d, 'row-%d')", i*10+j, j, j)
		}
		mustExec(b, db, sb.String())
		// Reset table to keep consistent state.
		mustExec(b, db, "DELETE FROM t")
	}
}

// BenchmarkBatchInsert_100 benchmarks inserting 100 rows in a single INSERT.
func BenchmarkBatchInsert_100(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")

	// Pre-build the SQL once (same values each iteration, tests throughput not alloc).
	var sb strings.Builder
	sb.WriteString("INSERT INTO t VALUES ")
	for j := 0; j < 100; j++ {
		if j > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "(%d, %d, 'row-%d')", j, j%10, j)
	}
	batchSQL := sb.String()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExec(b, db, batchSQL)
		mustExec(b, db, "DELETE FROM t")
	}
}

// BenchmarkBatchInsert_1000 benchmarks inserting 1000 rows in a single INSERT.
func BenchmarkBatchInsert_1000(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")

	var sb strings.Builder
	sb.WriteString("INSERT INTO t VALUES ")
	for j := 0; j < 1000; j++ {
		if j > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "(%d, %d, 'row-%d')", j, j%100, j)
	}
	batchSQL := sb.String()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExec(b, db, batchSQL)
		mustExec(b, db, "DELETE FROM t")
	}
}

// BenchmarkSingleInsert compares single-row INSERT as a baseline.
func BenchmarkSingleInsert(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, 'row')", i, i))
	}
}

// -----------------------------------------------------------------
// Phase 2: Allocation benchmarks for SELECT hot path
// -----------------------------------------------------------------

// BenchmarkSelectAllocs_Simple measures allocations for a simple SELECT query.
func BenchmarkSelectAllocs_Simple(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, 'name-%d')", i, i, i))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t")
	}
}

// BenchmarkSelectAllocs_WithOrderBy measures allocations for ORDER BY queries
// (exercises the pooled selectColSet and colIndices code paths).
func BenchmarkSelectAllocs_WithOrderBy(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, name TEXT)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, 'name-%d')", i, i, i))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT id, name FROM t ORDER BY val DESC LIMIT 10")
	}
}

// BenchmarkCountStar_FastPath measures the HybridStore COUNT(*) fast path.
func BenchmarkCountStar_FastPath(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}
