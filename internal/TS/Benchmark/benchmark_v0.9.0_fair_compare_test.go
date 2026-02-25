// Package Benchmark provides fair (no-result-cache) comparison benchmarks for
// the v0.9.0 README performance table.
//
// The benchmarks in benchmark_test.go and benchmark_sqlite_test.go run the same
// SQL string every iteration, allowing sqlvibe's in-process result cache to serve
// all but the first call.  That measures cache-hit throughput, not per-query
// execution latency.
//
// The benchmarks in this file call db.ClearResultCache() before each iteration
// (via b.StopTimer()/b.StartTimer()) so both engines are measured on actual
// query execution with plan-cache (bytecode / prepared-statement) warm:
//   - sqlvibe:  plan cache HOT, result cache COLD  → real execution cost
//   - SQLite:   prepared-statement cache HOT        → real execution cost
//
// This gives an apples-to-apples comparison that is suitable for the README.
package Benchmark

import (
	"fmt"
	"testing"
)

// -----------------------------------------------------------------
// SELECT * full table scan (1 000 rows, 3 cols)
// -----------------------------------------------------------------

func BenchmarkFair_SelectAll(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, name TEXT, score REAL)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'name%d', %f)", i, i, float64(i)*1.1))
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

func BenchmarkFair_SQLite_SelectAll(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER, name TEXT, score REAL)")
	for i := 0; i < 1000; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'name%d', %f)", i, i, float64(i)*1.1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT * FROM t")
		for rows.Next() {
		}
		rows.Close()
	}
}

// -----------------------------------------------------------------
// SELECT with WHERE filter (1 000 rows)
// -----------------------------------------------------------------

func BenchmarkFair_SelectWhere(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT * FROM t WHERE val = 5")
		for rows.Next() {
		}
	}
}

func BenchmarkFair_SQLite_SelectWhere(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT * FROM t WHERE val = 5")
		for rows.Next() {
		}
		rows.Close()
	}
}

// -----------------------------------------------------------------
// SELECT with ORDER BY (500 rows)
// -----------------------------------------------------------------

func BenchmarkFair_SelectOrderBy(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, score INTEGER)")
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, 500-i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT * FROM t ORDER BY score ASC")
		for rows.Next() {
		}
	}
}

func BenchmarkFair_SQLite_SelectOrderBy(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER, score INTEGER)")
	for i := 0; i < 500; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, 500-i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT * FROM t ORDER BY score ASC")
		for rows.Next() {
		}
		rows.Close()
	}
}

// -----------------------------------------------------------------
// COUNT(*) aggregate (1 000 rows)
// -----------------------------------------------------------------

func BenchmarkFair_Count(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 1; i <= 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
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

func BenchmarkFair_SQLite_Count(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 1; i <= 1000; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT COUNT(*) FROM t")
		for rows.Next() {
		}
		rows.Close()
	}
}

// -----------------------------------------------------------------
// SUM aggregate (1 000 rows)
// -----------------------------------------------------------------

func BenchmarkFair_Sum(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 1; i <= 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
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

func BenchmarkFair_SQLite_Sum(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 1; i <= 1000; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT SUM(val) FROM t")
		for rows.Next() {
		}
		rows.Close()
	}
}

// -----------------------------------------------------------------
// GROUP BY (1 000 rows)
// -----------------------------------------------------------------

func BenchmarkFair_GroupBy(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE sales (region TEXT, amount INTEGER)")
	regions := []string{"North", "South", "East", "West"}
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO sales VALUES ('%s', %d)", regions[i%4], i+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT region, SUM(amount), COUNT(*), AVG(amount) FROM sales GROUP BY region ORDER BY region")
		for rows.Next() {
		}
	}
}

func BenchmarkFair_SQLite_GroupBy(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE sales (region TEXT, amount INTEGER)")
	regions := []string{"North", "South", "East", "West"}
	for i := 0; i < 1000; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO sales VALUES ('%s', %d)", regions[i%4], i+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT region, SUM(amount), COUNT(*), AVG(amount) FROM sales GROUP BY region ORDER BY region")
		for rows.Next() {
		}
		rows.Close()
	}
}

// -----------------------------------------------------------------
// INNER JOIN (100 users × 500 orders)
// -----------------------------------------------------------------

func BenchmarkFair_Join(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(b, db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)")
	for i := 1; i <= 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO users VALUES (%d, 'user%d')", i, i))
	}
	for i := 1; i <= 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %d)", i, (i%100)+1, i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT u.name, o.amount FROM users AS u JOIN orders AS o ON u.id = o.user_id WHERE o.amount > 1000")
		for rows.Next() {
		}
	}
}

func BenchmarkFair_SQLite_Join(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	mustExecSQLite(b, db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount INTEGER)")
	for i := 1; i <= 100; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO users VALUES (%d, 'user%d')", i, i))
	}
	for i := 1; i <= 500; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %d)", i, (i%100)+1, i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT u.name, o.amount FROM users AS u JOIN orders AS o ON u.id = o.user_id WHERE o.amount > 1000")
		for rows.Next() {
		}
		rows.Close()
	}
}

// -----------------------------------------------------------------
// BETWEEN filter (1 000 rows)
// -----------------------------------------------------------------

func BenchmarkFair_Between(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, score INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT id FROM t WHERE score BETWEEN 100 AND 800")
		for rows.Next() {
		}
	}
}

func BenchmarkFair_SQLite_Between(b *testing.B) {
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

// -----------------------------------------------------------------
// Result cache hit (same query, no clear — measures cache benefit)
// -----------------------------------------------------------------

func BenchmarkFair_ResultCacheHit(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, name TEXT, score REAL)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'name%d', %f)", i, i, float64(i)*1.1))
	}
	// Warm the result cache with one full call (iterate rows so the cache stores the result).
	warmRows := mustQuery(b, db, "SELECT * FROM t")
	for warmRows.Next() {
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// No ClearResultCache → measures pure cache-hit latency.
		rows := mustQuery(b, db, "SELECT * FROM t")
		for rows.Next() {
		}
	}
}
