// Package Benchmark provides SQL-level performance benchmarks for sqlvibe.
// This file contains v0.7.4 benchmarks for the hash index, PK set, and dedup fixes.
package Benchmark

import (
	"fmt"
	"testing"
)

// -----------------------------------------------------------------
// Wave 10 (v0.7.4): Hash Index & PK Hash Set
// Focus: Validate O(1) primary-key uniqueness check and secondary
//         index hash lookup.
// -----------------------------------------------------------------

// BenchmarkInsertBatchPK measures batch insert throughput where each row
// has a unique PRIMARY KEY.  The PK hash set replaces the old O(N) scan,
// so throughput should be constant rather than degrading as the table grows.
func BenchmarkInsertBatchPK(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mustExec(b, db, "DELETE FROM t")
		b.StartTimer()
		for j := 0; j < 1000; j++ {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'v%d')", j, j))
		}
	}
}

// BenchmarkSecondaryIndexLookup measures SELECT throughput with an equality
// WHERE on a secondary-indexed column.  The hash index reduces this from
// O(N) table scan to O(1) hash lookup + VM execution on the result set.
func BenchmarkSecondaryIndexLookup(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE products (id INTEGER PRIMARY KEY, category TEXT, price INTEGER)")
	mustExec(b, db, "CREATE INDEX idx_category ON products(category)")

	// Insert 1 000 rows across 10 categories (100 per category).
	categories := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	for i := 0; i < 1000; i++ {
		cat := categories[i%10]
		mustExec(b, db, fmt.Sprintf("INSERT INTO products VALUES (%d, '%s', %d)", i, cat, i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cat := categories[i%10]
		mustQuery(b, db, fmt.Sprintf("SELECT * FROM products WHERE category = '%s'", cat))
	}
}

// BenchmarkSecondaryIndexLookupUnique measures a unique-value secondary index
// lookup (1 matching row out of 1 000).
func BenchmarkSecondaryIndexLookupUnique(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE employees (id INTEGER PRIMARY KEY, email TEXT, dept TEXT)")
	mustExec(b, db, "CREATE UNIQUE INDEX idx_email ON employees(email)")

	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO employees VALUES (%d, 'user%d@corp.com', 'dept%d')", i, i, i%10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, fmt.Sprintf("SELECT * FROM employees WHERE email = 'user%d@corp.com'", i%1000))
	}
}

// BenchmarkDeduplicateRows measures the improved deduplicateRows function
// (strings.Builder + type switch) vs old fmt.Sprintf path.
func BenchmarkDeduplicateRows(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val TEXT)")
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'v%d')", i, i))
	}

	b.ResetTimer()
	// UNION ALL causes deduplicateRows to be called on the combined result set.
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT id, val FROM t UNION SELECT id, val FROM t")
	}
}
