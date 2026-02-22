// Package Benchmark provides SQL-level performance benchmarks for sqlvibe.
// This file contains v0.7.8 SQLite comparison benchmarks: side-by-side
// measurements of sqlvibe vs. go-sqlite for the same queries, to document the
// speedup/regression from v0.7.8 optimizations (plan cache, result cache,
// branch prediction, predicate pushdown, top-N heap).
package Benchmark

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/glebarez/go-sqlite"
)

// -----------------------------------------------------------------
// Helpers (sqlvibe side already defined in benchmark_test.go)
// -----------------------------------------------------------------

func openSQLite78(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatalf("Failed to open SQLite: %v", err)
	}
	return db
}

func execSQLite78(b *testing.B, db *sql.DB, query string) {
	b.Helper()
	if _, err := db.Exec(query); err != nil {
		b.Fatalf("SQLite Exec(%q) failed: %v", query, err)
	}
}

func querySQLite78(b *testing.B, db *sql.DB, query string) {
	b.Helper()
	rows, err := db.Query(query)
	if err != nil {
		b.Fatalf("SQLite Query(%q) failed: %v", query, err)
	}
	for rows.Next() {
	}
	rows.Close()
}

// -----------------------------------------------------------------
// WHERE filtering comparison (1 000 rows)
// -----------------------------------------------------------------

// BenchmarkSQLite78_WhereFiltering is the SQLite baseline for WHERE clause
// performance on a 1 000-row table.
func BenchmarkSQLite78_WhereFiltering(b *testing.B) {
	db := openSQLite78(b)
	defer db.Close()

	execSQLite78(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)")
	for i := 0; i < 1000; i++ {
		execSQLite78(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		querySQLite78(b, db, "SELECT * FROM t WHERE x > 500")
	}
}

// BenchmarkSqlvibe78_WhereFiltering measures sqlvibe WHERE clause performance
// on a 1 000-row table.  With predicate pushdown, simple conditions are
// evaluated at the Go layer before the VM, reducing rows the VM must process.
func BenchmarkSqlvibe78_WhereFiltering(b *testing.B) {
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

// -----------------------------------------------------------------
// COUNT(*) comparison (1 000 rows)
// -----------------------------------------------------------------

func BenchmarkSQLite78_CountStar(b *testing.B) {
	db := openSQLite78(b)
	defer db.Close()

	execSQLite78(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, x INTEGER)")
	for i := 0; i < 1000; i++ {
		execSQLite78(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		querySQLite78(b, db, "SELECT COUNT(*) FROM t")
	}
}

func BenchmarkSqlvibe78_CountStar(b *testing.B) {
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

// -----------------------------------------------------------------
// ORDER BY + LIMIT 10 (Top-N) comparison (10 000 rows)
// -----------------------------------------------------------------

func BenchmarkSQLite78_TopN_Limit10(b *testing.B) {
	db := openSQLite78(b)
	defer db.Close()

	execSQLite78(b, db, "CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER)")
	for i := 0; i < 10000; i++ {
		execSQLite78(b, db, fmt.Sprintf("INSERT INTO events VALUES (%d, %d)", i, i*37%10000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		querySQLite78(b, db, "SELECT id, ts FROM events ORDER BY ts DESC LIMIT 10")
	}
}

func BenchmarkSqlvibe78_TopN_Limit10(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE events (id INTEGER PRIMARY KEY, ts INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO events VALUES (%d, %d)", i, i*37%10000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT id, ts FROM events ORDER BY ts DESC LIMIT 10")
		_ = rows
	}
}

// -----------------------------------------------------------------
// Result cache comparison: repeated identical SELECT
// -----------------------------------------------------------------

// BenchmarkSQLite78_ResultCache_Hit measures SQLite's prepared-statement
// cache hit path for a repeated identical query.
func BenchmarkSQLite78_ResultCache_Hit(b *testing.B) {
	db := openSQLite78(b)
	defer db.Close()

	execSQLite78(b, db, "CREATE TABLE metrics (id INTEGER PRIMARY KEY, score REAL)")
	for i := 0; i < 500; i++ {
		execSQLite78(b, db, fmt.Sprintf("INSERT INTO metrics VALUES (%d, %.2f)", i, float64(i)*1.5))
	}

	// Warm SQLite's plan cache
	querySQLite78(b, db, "SELECT id, score FROM metrics WHERE score > 100.0")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		querySQLite78(b, db, "SELECT id, score FROM metrics WHERE score > 100.0")
	}
}

// BenchmarkSqlvibe78_ResultCache_Hit measures sqlvibe's full result cache hit
// path for a repeated identical query.
func BenchmarkSqlvibe78_ResultCache_Hit(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE metrics (id INTEGER PRIMARY KEY, score REAL)")
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO metrics VALUES (%d, %.2f)", i, float64(i)*1.5))
	}

	// Warm sqlvibe's result cache
	mustQuery(b, db, "SELECT id, score FROM metrics WHERE score > 100.0")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT id, score FROM metrics WHERE score > 100.0")
		_ = rows
	}
}

// -----------------------------------------------------------------
// INNER JOIN comparison (100 × 100 rows)
// -----------------------------------------------------------------

func BenchmarkSQLite78_InnerJoin(b *testing.B) {
	db := openSQLite78(b)
	defer db.Close()

	execSQLite78(b, db, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, val TEXT)")
	execSQLite78(b, db, "CREATE TABLE t2 (id INTEGER PRIMARY KEY, score INTEGER)")
	for i := 0; i < 100; i++ {
		execSQLite78(b, db, fmt.Sprintf("INSERT INTO t1 VALUES (%d, 'v%d')", i, i))
		execSQLite78(b, db, fmt.Sprintf("INSERT INTO t2 VALUES (%d, %d)", i, i*5))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		querySQLite78(b, db, "SELECT t1.id, t1.val, t2.score FROM t1 JOIN t2 ON t1.id = t2.id")
	}
}

func BenchmarkSqlvibe78_InnerJoin(b *testing.B) {
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

// -----------------------------------------------------------------
// GROUP BY comparison (1 000 rows, 4 groups)
// -----------------------------------------------------------------

func BenchmarkSQLite78_GroupBy(b *testing.B) {
	db := openSQLite78(b)
	defer db.Close()

	execSQLite78(b, db, "CREATE TABLE sales (region TEXT, amount INTEGER)")
	regions := []string{"North", "South", "East", "West"}
	for i := 0; i < 1000; i++ {
		execSQLite78(b, db, fmt.Sprintf("INSERT INTO sales VALUES ('%s', %d)", regions[i%4], i+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		querySQLite78(b, db, "SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region ORDER BY region")
	}
}

func BenchmarkSqlvibe78_GroupBy(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE sales (region TEXT, amount INTEGER)")
	regions := []string{"North", "South", "East", "West"}
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO sales VALUES ('%s', %d)", regions[i%4], i+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region ORDER BY region")
		_ = rows
	}
}

// -----------------------------------------------------------------
// Predicate pushdown comparison (10 000 rows, no index)
// -----------------------------------------------------------------

// BenchmarkSQLite78_PredicatePushdown measures SQLite WHERE scan on a
// 10 000-row table without an index.
func BenchmarkSQLite78_PredicatePushdown(b *testing.B) {
	db := openSQLite78(b)
	defer db.Close()

	execSQLite78(b, db, "CREATE TABLE big (id INTEGER, x INTEGER, y INTEGER)")
	for i := 0; i < 10000; i++ {
		execSQLite78(b, db, fmt.Sprintf("INSERT INTO big VALUES (%d, %d, %d)", i, i%1000, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		querySQLite78(b, db, "SELECT * FROM big WHERE x > 500 AND y < 50")
	}
}

// BenchmarkSqlvibe78_PredicatePushdown measures sqlvibe WHERE performance on
// 10 000 rows with predicate pushdown: both `x > 500` and `y < 50` are
// evaluated at the Go layer (before the VM), so the VM only sees the ~4 950
// matching rows.
func BenchmarkSqlvibe78_PredicatePushdown(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE big (id INTEGER, x INTEGER, y INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO big VALUES (%d, %d, %d)", i, i%1000, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuery(b, db, "SELECT * FROM big WHERE x > 500 AND y < 50")
		_ = rows
	}
}
