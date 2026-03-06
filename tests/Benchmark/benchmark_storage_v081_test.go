// Package Benchmark provides SQL-level performance benchmarks for sqlvibe.
// This file contains v0.8.1 storage-layer benchmarks for columnar operations,
// parallel aggregation, and parallel scan — all now routed through the C++ engine
// (src/core/DS/) via the public SQL API in v0.11.2+.
package Benchmark

import (
	"fmt"
	"testing"
)

// -----------------------------------------------------------------
// Phase 1: Columnar opcode benchmarks (1K rows)
// -----------------------------------------------------------------

// BenchmarkColumnarScan_1K benchmarks a full table scan of 1K rows.
func BenchmarkColumnarScan_1K(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT * FROM t")
		for rows.Next() {
		}
	}
}

// BenchmarkColumnarFilter_1K benchmarks a range filter scan of 1K rows.
func BenchmarkColumnarFilter_1K(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT * FROM t WHERE val >= 500")
		for rows.Next() {
		}
	}
}

// BenchmarkColumnarCount_1K benchmarks COUNT(*) on 1K rows.
func BenchmarkColumnarCount_1K(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT COUNT(*) FROM t")
		for rows.Next() {
		}
	}
}

// BenchmarkColumnarSum_1K benchmarks SUM on 1K rows.
func BenchmarkColumnarSum_1K(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT SUM(val) FROM t")
		for rows.Next() {
		}
	}
}

// -----------------------------------------------------------------
// Phase 7: Parallel scan benchmarks (100K rows)
// -----------------------------------------------------------------

// BenchmarkParallelCount_100K benchmarks COUNT(*) on 100K rows.
func BenchmarkParallelCount_100K(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 100000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT COUNT(*) FROM t")
		for rows.Next() {
		}
	}
}

// BenchmarkParallelSum_100K benchmarks SUM on 100K rows.
func BenchmarkParallelSum_100K(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 100000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT SUM(val) FROM t")
		for rows.Next() {
		}
	}
}

// BenchmarkParallelScan_100K benchmarks a full table scan of 100K rows.
func BenchmarkParallelScan_100K(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 100000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT id, val FROM t")
		for rows.Next() {
		}
	}
}
