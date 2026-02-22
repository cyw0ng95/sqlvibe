// Package Benchmark provides v0.9.0 performance benchmarks.
// These benchmarks cover the v0.9.0 optimizations:
//   - BETWEEN predicate pushdown
//   - Fast Hash JOIN (direct value comparison without string allocation)
//   - Extension framework overhead
package Benchmark

import (
	"fmt"
	"testing"
)

// -----------------------------------------------------------------
// BETWEEN predicate pushdown (v0.9.0 Optimization)
// -----------------------------------------------------------------

// BenchmarkBetween_Pushdown measures WHERE x BETWEEN lo AND hi
// on a 1000-row table.  The BETWEEN predicate is now pushed down
// to the Go layer before VM execution.
func BenchmarkBetween_Pushdown(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, score INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT id FROM t WHERE score BETWEEN 100 AND 800")
		_ = rows
	}
}

// BenchmarkBetween_NoPushdown measures a similar range filter WITHOUT
// BETWEEN (using >= AND <=) as a comparison baseline.
func BenchmarkBetween_EquivRange(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, score INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT id FROM t WHERE score >= 100 AND score <= 800")
		_ = rows
	}
}

// -----------------------------------------------------------------
// Fast Hash JOIN (v0.9.0 Optimization)
// -----------------------------------------------------------------

// BenchmarkFastHashJoin_Int measures a hash join where the join key is
// an INTEGER column.  The v0.9.0 optimization avoids fmt.Sprintf
// allocation by using the raw int64 value as the map key.
func BenchmarkFastHashJoin_Int(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE a (id INTEGER, val TEXT)")
	mustExec(b, db, "CREATE TABLE b (id INTEGER, score INTEGER)")
	for i := 0; i < 200; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO a VALUES (%d, 'a%d')", i, i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO b VALUES (%d, %d)", i, i*3))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT a.val, b.score FROM a JOIN b ON a.id = b.id")
		_ = rows
	}
}

// BenchmarkFastHashJoin_String measures a hash join where the join key is
// a TEXT column — exercises the string path of the fast hash key.
func BenchmarkFastHashJoin_String(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE a (code TEXT, val INTEGER)")
	mustExec(b, db, "CREATE TABLE b (code TEXT, label TEXT)")
	for i := 0; i < 200; i++ {
		code := fmt.Sprintf("CODE-%04d", i)
		mustExec(b, db, fmt.Sprintf("INSERT INTO a VALUES ('%s', %d)", code, i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO b VALUES ('%s', 'label-%d')", code, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT a.val, b.label FROM a JOIN b ON a.code = b.code")
		_ = rows
	}
}

// -----------------------------------------------------------------
// Extension framework (v0.9.0)
// -----------------------------------------------------------------

// BenchmarkExtensionQuery measures the overhead of a query that touches
// the sqlvibe_extensions virtual table.
func BenchmarkExtensionQuery(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT * FROM sqlvibe_extensions")
		_ = rows
	}
}

// -----------------------------------------------------------------
// BETWEEN with SQLite comparison (v0.9.0)
// -----------------------------------------------------------------

// BenchmarkSQLite_Between measures SQLite's WHERE x BETWEEN lo AND hi
// on a 1000-row table for direct comparison with the sqlvibe BETWEEN
// pushdown benchmark.
func BenchmarkSQLite_Between(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER, score INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT id FROM t WHERE score BETWEEN 100 AND 800")
		for rows.Next() {
		}
		rows.Close()
	}
}

// BenchmarkSQLite_HashJoin_Int measures SQLite's JOIN on INTEGER key for comparison.
func BenchmarkSQLite_HashJoin_Int(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE a (id INTEGER, val TEXT)")
	mustExecSQLite(b, db, "CREATE TABLE b (id INTEGER, score INTEGER)")
	for i := 0; i < 200; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO a VALUES (%d, 'a%d')", i, i))
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO b VALUES (%d, %d)", i, i*3))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT a.val, b.score FROM a JOIN b ON a.id = b.id")
		for rows.Next() {
		}
		rows.Close()
	}
}
