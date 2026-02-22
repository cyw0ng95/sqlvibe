// Package Benchmark provides SQL-level performance benchmarks for sqlvibe.
// This file contains v0.7.8 benchmarks focusing on VM & CG performance
// optimizations: branch prediction, result cache, plan cache, and top-N.
package Benchmark

import (
	"fmt"
	"testing"
)

// -----------------------------------------------------------------
// Branch Prediction (v0.7.8)
// BenchmarkBranchPrediction measures OpNext loop throughput with the
// 2-bit saturating-counter branch predictor warm vs. cold.
// -----------------------------------------------------------------

// BenchmarkBranchPrediction_WarmLoop measures a full-table scan on a table
// that fits entirely in memory. After the first iteration the predictor's
// saturating counter reaches "strongly taken", and subsequent iterations
// should benefit from the fast path in the OpNext handler.
func BenchmarkBranchPrediction_WarmLoop(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*2))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT id FROM t WHERE val > 500")
		_ = rows
	}
}

// BenchmarkBranchPrediction_ShortLoop measures a scan that terminates early
// (LIMIT 10) to exercise the "not taken" prediction path.
func BenchmarkBranchPrediction_ShortLoop(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*2))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT id FROM t LIMIT 10")
		_ = rows
	}
}

// -----------------------------------------------------------------
// Plan Cache (v0.7.8)
// BenchmarkPlanCache_Hit measures Query throughput when the compiled
// plan is already in the cache vs. the cold path.
// -----------------------------------------------------------------

// BenchmarkPlanCache_Hit executes the same SELECT query repeatedly.
// After the first call, the plan should be served from the cache,
// bypassing the tokenise+parse+compile pipeline.
func BenchmarkPlanCache_Hit(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d)", i, i*10))
	}

	// Warm the plan cache with one execution.
	mustQuery(b, db, "SELECT id, amount FROM orders WHERE amount > 500")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT id, amount FROM orders WHERE amount > 500")
		_ = rows
	}
}

// -----------------------------------------------------------------
// Result Cache (v0.7.8)
// BenchmarkResultCache_Hit measures Query throughput when the full
// query result is already in the result cache.
// -----------------------------------------------------------------

// BenchmarkResultCache_Hit executes the same read-only SELECT repeatedly.
// After the first call, subsequent calls should be served from the
// in-process result cache without any VM execution overhead.
func BenchmarkResultCache_Hit(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE metrics (id INTEGER PRIMARY KEY, score REAL)")
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO metrics VALUES (%d, %.2f)", i, float64(i)*1.5))
	}

	// Warm the result cache with one execution.
	mustQuery(b, db, "SELECT id, score FROM metrics WHERE score > 100.0")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT id, score FROM metrics WHERE score > 100.0")
		_ = rows
	}
}

// BenchmarkResultCache_Miss measures the baseline (cold cache) query latency
// for comparison with BenchmarkResultCache_Hit.
func BenchmarkResultCache_Miss(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE metrics (id INTEGER PRIMARY KEY, score REAL)")
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO metrics VALUES (%d, %.2f)", i, float64(i)*1.5))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Force cache miss by inserting a row each iteration, which invalidates the cache.
		b.StopTimer()
		mustExec(b, db, fmt.Sprintf("INSERT INTO metrics VALUES (%d, %.2f)", 10000+i, 9999.0))
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT id, score FROM metrics WHERE score > 100.0")
		_ = rows
	}
}

// -----------------------------------------------------------------
// Top-N Optimization (v0.7.8)
// BenchmarkTopN_Limit measures ORDER BY + LIMIT performance using the
// bounded heap path versus a naive full sort.
// -----------------------------------------------------------------

// BenchmarkTopN_Limit10 measures ORDER BY + LIMIT 10 on 10 000 rows.
// The heap-based TopN implementation retains only 10 rows in memory,
// giving O(N log 10) time vs. O(N log N) for a full sort.
func BenchmarkTopN_Limit10(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER, payload TEXT)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO events VALUES (%d, %d, 'data-%d')", i, i*37%10000, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT id, ts FROM events ORDER BY ts DESC LIMIT 10")
		_ = rows
	}
}

// BenchmarkTopN_Limit100 measures ORDER BY + LIMIT 100 for a slightly larger top-N window.
func BenchmarkTopN_Limit100(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER, payload TEXT)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO events VALUES (%d, %d, 'data-%d')", i, i*37%10000, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT id, ts FROM events ORDER BY ts DESC LIMIT 100")
		_ = rows
	}
}

// -----------------------------------------------------------------
// String Interning (v0.7.8)
// BenchmarkStringInterning measures the throughput of InternString on a
// mix of repeated and novel strings.
// -----------------------------------------------------------------

// BenchmarkStringInterning_Repeated tests InternString with the same
// small set of repeated strings (the common hot-path case where interning
// saves memory and speeds up equality checks).
func BenchmarkStringInterning_Repeated(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE tags (id INTEGER PRIMARY KEY, tag TEXT)")
	tags := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for i := 0; i < 1000; i++ {
		tag := tags[i%len(tags)]
		mustExec(b, db, fmt.Sprintf("INSERT INTO tags VALUES (%d, '%s')", i, tag))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT DISTINCT tag FROM tags")
		_ = rows
	}
}

// -----------------------------------------------------------------
// WHERE Filtering (v0.7.8 – comparison baseline)
// -----------------------------------------------------------------

// BenchmarkWhereFiltering_1K tests WHERE clause performance on 1000 rows.
func BenchmarkWhereFiltering_1K(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT * FROM t WHERE x > 500")
		_ = rows
	}
}

// BenchmarkCountStar_1K tests COUNT(*) performance on a 1000-row table.
func BenchmarkCountStar_1K(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT COUNT(*) FROM t")
		_ = rows
	}
}

// BenchmarkCountStarWhere_1K tests COUNT(*) with a WHERE clause on 1000 rows.
func BenchmarkCountStarWhere_1K(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT COUNT(*) FROM t WHERE x > 500")
		_ = rows
	}
}

// BenchmarkJoinTwoTables measures INNER JOIN throughput on two 100-row tables.
func BenchmarkJoinTwoTables(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	mustExec(b, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, score INTEGER)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t1 VALUES (%d, 'v%d')", i, i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO t2 VALUES (%d, %d)", i, i*5))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT t1.id, t1.val, t2.score FROM t1 JOIN t2 ON t1.id = t2.id")
		_ = rows
	}
}

// BenchmarkSubqueryIN measures IN subquery throughput.
func BenchmarkSubqueryIN(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)")
	mustExec(b, db, "CREATE TABLE sub (id INTEGER PRIMARY KEY)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO sub VALUES (%d)", i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT * FROM t WHERE id IN (SELECT id FROM sub)")
		_ = rows
	}
}
