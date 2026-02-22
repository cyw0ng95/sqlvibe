// Package Benchmark provides SQLite comparison benchmarks for complex SQL queries.
// This file tests the same queries against SQLite to compare performance.
package Benchmark

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/glebarez/go-sqlite"
)

// openSQLite opens an in-memory SQLite database for benchmarking.
func openSQLite(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatalf("Failed to open SQLite: %v", err)
	}
	return db
}

// sqliteMustExec runs a SQL statement and fails the benchmark on error.
func sqliteMustExec(b *testing.B, db *sql.DB, sql string) {
	b.Helper()
	if _, err := db.Exec(sql); err != nil {
		b.Fatalf("Exec(%q) failed: %v", sql, err)
	}
}

// sqliteMustQuery runs a SQL query, iterates all rows, and fails on error.
// Iterating all rows is required for a fair comparison with sqlvibe which
// eagerly materialises every result row inside Query().
func sqliteMustQuery(b *testing.B, db *sql.DB, query string) {
	b.Helper()
	rows, err := db.Query(query)
	if err != nil {
		b.Fatalf("Query(%q) failed: %v", query, err)
	}
	for rows.Next() {
		// Drive the cursor to completion; no column values needed for timing.
	}
	rows.Close()
}

// -----------------------------------------------------------------
// LARGE TABLE BENCHMARKS (100K rows)
// -----------------------------------------------------------------

// BenchmarkSQLiteComplex_SelectAll_100K measures full table scan on 100K rows
func BenchmarkSQLiteComplex_SelectAll_100K(b *testing.B) {
	db := openSQLite(b)
	defer db.Close()

	sqliteMustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, name TEXT, amount REAL)")
	for i := 0; i < 100000; i++ {
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, 'name-%d', %.2f)", i, i%1000, i, float64(i)*1.5))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqliteMustQuery(b, db, "SELECT * FROM t")
	}
}

// BenchmarkSQLiteComplex_CountStar_100K measures COUNT(*) on 100K rows
func BenchmarkSQLiteComplex_CountStar_100K(b *testing.B) {
	db := openSQLite(b)
	defer db.Close()

	sqliteMustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 0; i < 100000; i++ {
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqliteMustQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

// BenchmarkSQLiteComplex_Sum_100K measures SUM on 100K rows
func BenchmarkSQLiteComplex_Sum_100K(b *testing.B) {
	db := openSQLite(b)
	defer db.Close()

	sqliteMustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER)")
	for i := 0; i < 100000; i++ {
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%1000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqliteMustQuery(b, db, "SELECT SUM(amount) FROM t")
	}
}

// BenchmarkSQLiteComplex_Avg_100K measures AVG on 100K rows
func BenchmarkSQLiteComplex_Avg_100K(b *testing.B) {
	db := openSQLite(b)
	defer db.Close()

	sqliteMustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER)")
	for i := 0; i < 100000; i++ {
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%1000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqliteMustQuery(b, db, "SELECT AVG(amount) FROM t")
	}
}

// -----------------------------------------------------------------
// GROUP BY & AGGREGATES
// -----------------------------------------------------------------

// BenchmarkSQLiteComplex_GroupBy_MultiColumn measures GROUP BY on multiple columns
func BenchmarkSQLiteComplex_GroupBy_MultiColumn(b *testing.B) {
	db := openSQLite(b)
	defer db.Close()

	sqliteMustExec(b, db, "CREATE TABLE t (id INTEGER, region TEXT, category TEXT, amount INTEGER)")
	for i := 0; i < 10000; i++ {
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'region-%d', 'cat-%d', %d)",
			i, i%10, i%20, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqliteMustQuery(b, db, "SELECT region, category, SUM(amount), COUNT(*) FROM t GROUP BY region, category")
	}
}

// BenchmarkSQLiteComplex_GroupBy_Having measures GROUP BY with HAVING
func BenchmarkSQLiteComplex_GroupBy_Having(b *testing.B) {
	db := openSQLite(b)
	defer db.Close()

	sqliteMustExec(b, db, "CREATE TABLE t (id INTEGER, dept TEXT, salary INTEGER)")
	for i := 0; i < 10000; i++ {
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'dept-%d', %d)", i, i%50, 30000+i%20000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqliteMustQuery(b, db, "SELECT dept, AVG(salary) as avg_sal FROM t GROUP BY dept HAVING AVG(salary) > 40000")
	}
}

// BenchmarkSQLiteComplex_Aggregates_Multiple measures multiple aggregates in one query
func BenchmarkSQLiteComplex_Aggregates_Multiple(b *testing.B) {
	db := openSQLite(b)
	defer db.Close()

	sqliteMustExec(b, db, "CREATE TABLE t (id INTEGER, amount INTEGER)")
	for i := 0; i < 10000; i++ {
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%1000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqliteMustQuery(b, db, "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM t")
	}
}

// -----------------------------------------------------------------
// COMPLEX JOINS
// -----------------------------------------------------------------

// BenchmarkSQLiteComplex_Join_ThreeTable measures 3-table JOIN
func BenchmarkSQLiteComplex_Join_ThreeTable(b *testing.B) {
	db := openSQLite(b)
	defer db.Close()

	sqliteMustExec(b, db, "CREATE TABLE orders (id INTEGER, customer_id INTEGER, product_id INTEGER, amount INTEGER)")
	sqliteMustExec(b, db, "CREATE TABLE customers (id INTEGER, name TEXT, city TEXT)")
	sqliteMustExec(b, db, "CREATE TABLE products (id INTEGER, name TEXT, category TEXT)")

	for i := 0; i < 1000; i++ {
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %d, %d)", i, i%500, i%300, i%1000))
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO customers VALUES (%d, 'customer-%d', 'city-%d')", i, i, i%20))
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO products VALUES (%d, 'product-%d', 'cat-%d')", i, i, i%30))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqliteMustQuery(b, db, `
			SELECT o.id, c.name, p.name, o.amount 
			FROM orders o
			JOIN customers c ON o.customer_id = c.id
			JOIN products p ON o.product_id = p.id
		`)
	}
}

// -----------------------------------------------------------------
// ORDER BY & LIMIT
// -----------------------------------------------------------------

// BenchmarkSQLiteComplex_OrderBy_LargeLimit measures ORDER BY with large LIMIT
func BenchmarkSQLiteComplex_OrderBy_LargeLimit(b *testing.B) {
	db := openSQLite(b)
	defer db.Close()

	sqliteMustExec(b, db, "CREATE TABLE t (id INTEGER, value INTEGER)")
	for i := 0; i < 50000; i++ {
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%1000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqliteMustQuery(b, db, "SELECT * FROM t ORDER BY value DESC LIMIT 1000")
	}
}

// -----------------------------------------------------------------
// WINDOW FUNCTIONS
// -----------------------------------------------------------------

// BenchmarkSQLiteComplex_Window_RowNumber measures ROW_NUMBER() window function
func BenchmarkSQLiteComplex_Window_RowNumber(b *testing.B) {
	db := openSQLite(b)
	defer db.Close()

	sqliteMustExec(b, db, "CREATE TABLE t (id INTEGER, dept TEXT, salary INTEGER)")
	for i := 0; i < 10000; i++ {
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'dept-%d', %d)", i, i%20, 30000+i%20000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqliteMustQuery(b, db, "SELECT id, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM t")
	}
}

// -----------------------------------------------------------------
// UPDATE & DELETE
// -----------------------------------------------------------------

// BenchmarkSQLiteComplex_Update_LargeTable measures UPDATE on large table
func BenchmarkSQLiteComplex_Update_LargeTable(b *testing.B) {
	db := openSQLite(b)
	defer db.Close()

	sqliteMustExec(b, db, "CREATE TABLE t (id INTEGER, value INTEGER)")
	for i := 0; i < 50000; i++ {
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqliteMustExec(b, db, "UPDATE t SET value = value + 1 WHERE value > 50")
	}
}

// BenchmarkSQLiteComplex_Delete_LargeTable measures DELETE on large table
func BenchmarkSQLiteComplex_Delete_LargeTable(b *testing.B) {
	db := openSQLite(b)
	defer db.Close()

	sqliteMustExec(b, db, "CREATE TABLE t (id INTEGER, value INTEGER)")
	for i := 0; i < 50000; i++ {
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqliteMustExec(b, db, "DELETE FROM t WHERE value < 10")
	}
}

// -----------------------------------------------------------------
// DISTINCT & UNION
// -----------------------------------------------------------------

// BenchmarkSQLiteComplex_Distinct_Large measures DISTINCT on large dataset
func BenchmarkSQLiteComplex_Distinct_Large(b *testing.B) {
	db := openSQLite(b)
	defer db.Close()

	sqliteMustExec(b, db, "CREATE TABLE t (id INTEGER, category TEXT, region TEXT)")
	for i := 0; i < 100000; i++ {
		sqliteMustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'cat-%d', 'region-%d')", i, i%100, i%50))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqliteMustQuery(b, db, "SELECT DISTINCT category, region FROM t")
	}
}
