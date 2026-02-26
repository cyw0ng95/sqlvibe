// Package Benchmark provides v0.10.0 performance benchmarks comparing
// SQLite (go-sqlite) vs sqlvibe bytecode engine across multiple data scales
// (1 000, 10 000, 100 000 rows) and query patterns.
//
// Run with:
//
//	go test ./internal/TS/Benchmark/... -bench=BenchmarkCompare_ -benchmem -benchtime=3s
package Benchmark

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/glebarez/go-sqlite"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// ─── helpers ────────────────────────────────────────────────────────────────

func openSV(b *testing.B) *sqlvibe.Database {
	b.Helper()
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	return db
}

func openSQ(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatalf("open sqlite: %v", err)
	}
	return db
}

func svExec(b *testing.B, db *sqlvibe.Database, q string) {
	b.Helper()
	if _, err := db.Exec(q); err != nil {
		b.Fatalf("svExec(%q): %v", q, err)
	}
}

func sqExec(b *testing.B, db *sql.DB, q string) {
	b.Helper()
	if _, err := db.Exec(q); err != nil {
		b.Fatalf("sqExec(%q): %v", q, err)
	}
}

func svQuery(b *testing.B, db *sqlvibe.Database, q string) {
	b.Helper()
	rows, err := db.Query(q)
	if err != nil {
		b.Fatalf("svQuery(%q): %v", q, err)
	}
	for rows.Next() {
	}
}

func sqQuery(b *testing.B, db *sql.DB, q string) {
	b.Helper()
	rows, err := db.Query(q)
	if err != nil {
		b.Fatalf("sqQuery(%q): %v", q, err)
	}
	for rows.Next() {
	}
	rows.Close()
}

// seedIntegers inserts n rows (id INTEGER, val INTEGER) into tbl.
func seedSVIntegers(b *testing.B, db *sqlvibe.Database, tbl string, n int) {
	b.Helper()
	svExec(b, db, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, val INTEGER)", tbl))
	for i := 0; i < n; i++ {
		svExec(b, db, fmt.Sprintf("INSERT INTO %s VALUES (%d, %d)", tbl, i, i*3%n))
	}
}

func seedSQIntegers(b *testing.B, db *sql.DB, tbl string, n int) {
	b.Helper()
	sqExec(b, db, fmt.Sprintf("CREATE TABLE %s (id INTEGER PRIMARY KEY, val INTEGER)", tbl))
	for i := 0; i < n; i++ {
		sqExec(b, db, fmt.Sprintf("INSERT INTO %s VALUES (%d, %d)", tbl, i, i*3%n))
	}
}

// seedSales inserts n rows (id INTEGER, region TEXT, amount INTEGER) into tbl.
func seedSVSales(b *testing.B, db *sqlvibe.Database, tbl string, n int) {
	b.Helper()
	regions := []string{"North", "South", "East", "West"}
	svExec(b, db, fmt.Sprintf("CREATE TABLE %s (id INTEGER, region TEXT, amount INTEGER)", tbl))
	for i := 0; i < n; i++ {
		svExec(b, db, fmt.Sprintf("INSERT INTO %s VALUES (%d, '%s', %d)", tbl, i, regions[i%4], i+1))
	}
}

func seedSQSales(b *testing.B, db *sql.DB, tbl string, n int) {
	b.Helper()
	regions := []string{"North", "South", "East", "West"}
	sqExec(b, db, fmt.Sprintf("CREATE TABLE %s (id INTEGER, region TEXT, amount INTEGER)", tbl))
	for i := 0; i < n; i++ {
		sqExec(b, db, fmt.Sprintf("INSERT INTO %s VALUES (%d, '%s', %d)", tbl, i, regions[i%4], i+1))
	}
}

// ─── SELECT all ─────────────────────────────────────────────────────────────

func BenchmarkCompare_SelectAll_1K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT id, val FROM t")
	}
}

func BenchmarkCompare_SelectAll_1K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT id, val FROM t")
	}
}

func BenchmarkCompare_SelectAll_10K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT id, val FROM t")
	}
}

func BenchmarkCompare_SelectAll_10K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT id, val FROM t")
	}
}

func BenchmarkCompare_SelectAll_100K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT id, val FROM t")
	}
}

func BenchmarkCompare_SelectAll_100K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT id, val FROM t")
	}
}

// ─── WHERE filter ───────────────────────────────────────────────────────────

func BenchmarkCompare_WhereFilter_1K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT id, val FROM t WHERE val > 500")
	}
}

func BenchmarkCompare_WhereFilter_1K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT id, val FROM t WHERE val > 500")
	}
}

func BenchmarkCompare_WhereFilter_10K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT id, val FROM t WHERE val > 5000")
	}
}

func BenchmarkCompare_WhereFilter_10K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT id, val FROM t WHERE val > 5000")
	}
}

func BenchmarkCompare_WhereFilter_100K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT id, val FROM t WHERE val > 50000")
	}
}

func BenchmarkCompare_WhereFilter_100K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT id, val FROM t WHERE val > 50000")
	}
}

// ─── COUNT(*) ────────────────────────────────────────────────────────────────

func BenchmarkCompare_CountStar_1K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

func BenchmarkCompare_CountStar_1K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

func BenchmarkCompare_CountStar_10K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

func BenchmarkCompare_CountStar_10K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

func BenchmarkCompare_CountStar_100K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

func BenchmarkCompare_CountStar_100K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

// ─── SUM aggregate ──────────────────────────────────────────────────────────

func BenchmarkCompare_SumAggregate_1K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT SUM(val) FROM t")
	}
}

func BenchmarkCompare_SumAggregate_1K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT SUM(val) FROM t")
	}
}

func BenchmarkCompare_SumAggregate_10K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT SUM(val) FROM t")
	}
}

func BenchmarkCompare_SumAggregate_10K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT SUM(val) FROM t")
	}
}

func BenchmarkCompare_SumAggregate_100K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT SUM(val) FROM t")
	}
}

func BenchmarkCompare_SumAggregate_100K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT SUM(val) FROM t")
	}
}

// ─── GROUP BY ────────────────────────────────────────────────────────────────

func BenchmarkCompare_GroupBy_1K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQSales(b, db, "sales", 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region ORDER BY region")
	}
}

func BenchmarkCompare_GroupBy_1K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVSales(b, db, "sales", 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region ORDER BY region")
	}
}

func BenchmarkCompare_GroupBy_10K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQSales(b, db, "sales", 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region ORDER BY region")
	}
}

func BenchmarkCompare_GroupBy_10K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVSales(b, db, "sales", 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region ORDER BY region")
	}
}

func BenchmarkCompare_GroupBy_100K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQSales(b, db, "sales", 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region ORDER BY region")
	}
}

func BenchmarkCompare_GroupBy_100K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVSales(b, db, "sales", 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region ORDER BY region")
	}
}

// ─── ORDER BY + LIMIT (Top-N) ────────────────────────────────────────────────

func BenchmarkCompare_TopN_1K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT id, val FROM t ORDER BY val DESC LIMIT 10")
	}
}

func BenchmarkCompare_TopN_1K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT id, val FROM t ORDER BY val DESC LIMIT 10")
	}
}

func BenchmarkCompare_TopN_10K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT id, val FROM t ORDER BY val DESC LIMIT 10")
	}
}

func BenchmarkCompare_TopN_10K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT id, val FROM t ORDER BY val DESC LIMIT 10")
	}
}

func BenchmarkCompare_TopN_100K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQIntegers(b, db, "t", 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT id, val FROM t ORDER BY val DESC LIMIT 10")
	}
}

func BenchmarkCompare_TopN_100K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVIntegers(b, db, "t", 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT id, val FROM t ORDER BY val DESC LIMIT 10")
	}
}

// ─── INNER JOIN ──────────────────────────────────────────────────────────────

// seedJoin creates two tables: left (n rows) and right (n rows), joined on id.
func seedSVJoin(b *testing.B, db *sqlvibe.Database, n int) {
	b.Helper()
	svExec(b, db, "CREATE TABLE left_t (id INTEGER PRIMARY KEY, lval INTEGER)")
	svExec(b, db, "CREATE TABLE right_t (id INTEGER PRIMARY KEY, rval INTEGER)")
	for i := 0; i < n; i++ {
		svExec(b, db, fmt.Sprintf("INSERT INTO left_t VALUES (%d, %d)", i, i*2))
		svExec(b, db, fmt.Sprintf("INSERT INTO right_t VALUES (%d, %d)", i, i*3))
	}
}

func seedSQJoin(b *testing.B, db *sql.DB, n int) {
	b.Helper()
	sqExec(b, db, "CREATE TABLE left_t (id INTEGER PRIMARY KEY, lval INTEGER)")
	sqExec(b, db, "CREATE TABLE right_t (id INTEGER PRIMARY KEY, rval INTEGER)")
	for i := 0; i < n; i++ {
		sqExec(b, db, fmt.Sprintf("INSERT INTO left_t VALUES (%d, %d)", i, i*2))
		sqExec(b, db, fmt.Sprintf("INSERT INTO right_t VALUES (%d, %d)", i, i*3))
	}
}

func BenchmarkCompare_InnerJoin_1K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQJoin(b, db, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT l.id, l.lval, r.rval FROM left_t l JOIN right_t r ON l.id = r.id")
	}
}

func BenchmarkCompare_InnerJoin_1K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVJoin(b, db, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT l.id, l.lval, r.rval FROM left_t l JOIN right_t r ON l.id = r.id")
	}
}

func BenchmarkCompare_InnerJoin_10K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQJoin(b, db, 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT l.id, l.lval, r.rval FROM left_t l JOIN right_t r ON l.id = r.id")
	}
}

func BenchmarkCompare_InnerJoin_10K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVJoin(b, db, 10000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT l.id, l.lval, r.rval FROM left_t l JOIN right_t r ON l.id = r.id")
	}
}

func BenchmarkCompare_InnerJoin_100K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	seedSQJoin(b, db, 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT l.id, l.lval, r.rval FROM left_t l JOIN right_t r ON l.id = r.id")
	}
}

func BenchmarkCompare_InnerJoin_100K_SVBytecode(b *testing.B) {
	db := openSV(b)
	defer db.Close()
	seedSVJoin(b, db, 100000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		svQuery(b, db, "SELECT l.id, l.lval, r.rval FROM left_t l JOIN right_t r ON l.id = r.id")
	}
}

// ─── INSERT throughput ───────────────────────────────────────────────────────

func BenchmarkCompare_Insert_1K_SQLite(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := openSQ(b)
		sqExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
		b.StartTimer()
		for j := 0; j < 1000; j++ {
			sqExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
		}
		b.StopTimer()
		db.Close()
	}
}

func BenchmarkCompare_Insert_1K_SVBytecode(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := openSV(b)
		svExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
		b.StartTimer()
		for j := 0; j < 1000; j++ {
			svExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
		}
		b.StopTimer()
		db.Close()
	}
}

func BenchmarkCompare_Insert_10K_SQLite(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := openSQ(b)
		sqExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
		b.StartTimer()
		for j := 0; j < 10000; j++ {
			sqExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
		}
		b.StopTimer()
		db.Close()
	}
}

func BenchmarkCompare_Insert_10K_SVBytecode(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := openSV(b)
		svExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
		b.StartTimer()
		for j := 0; j < 10000; j++ {
			svExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
		}
		b.StopTimer()
		db.Close()
	}
}
