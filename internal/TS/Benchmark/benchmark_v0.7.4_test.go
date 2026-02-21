// Package Benchmark provides SQL-level performance benchmarks for sqlvibe.
// This file contains v0.7.3 and v0.7.4 benchmarks.
package Benchmark

import (
	"fmt"
	"testing"
)

// -----------------------------------------------------------------
// Wave 10 (v0.7.3): Hash Index & PK Hash Set
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

// -----------------------------------------------------------------
// Wave 11 (v0.7.4): Index Usage Expansion
// Focus: BETWEEN, IN-list, and LIKE-prefix index scans on secondary
//         indexes instead of full table scan.
// -----------------------------------------------------------------

// BenchmarkIndexBetween measures SELECT throughput with a BETWEEN WHERE on
// a secondary-indexed column.  The range scan replaces an O(N) table scan
// with an O(K) scan over index keys (K = distinct index values).
func BenchmarkIndexBetween(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE orders (id INTEGER PRIMARY KEY, amount INTEGER, region TEXT)")
	mustExec(b, db, "CREATE INDEX idx_amount ON orders(amount)")

	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, 'r%d')", i, i, i%10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM orders WHERE amount BETWEEN 100 AND 200")
	}
}

// BenchmarkIndexInList measures SELECT throughput with an IN-list WHERE on
// a secondary-indexed column.
func BenchmarkIndexInList(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE items (id INTEGER PRIMARY KEY, category TEXT, price INTEGER)")
	mustExec(b, db, "CREATE INDEX idx_cat ON items(category)")

	cats := []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J"}
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO items VALUES (%d, '%s', %d)", i, cats[i%10], i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM items WHERE category IN ('A', 'B', 'C')")
	}
}

// BenchmarkIndexLikePrefix measures SELECT throughput with a LIKE 'prefix%'
// WHERE on a secondary-indexed text column.
func BenchmarkIndexLikePrefix(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, score INTEGER)")
	mustExec(b, db, "CREATE INDEX idx_uname ON users(username)")

	prefixes := []string{"alice", "bob", "carol", "dave", "eve"}
	for i := 0; i < 1000; i++ {
		name := fmt.Sprintf("%s_%d", prefixes[i%5], i)
		mustExec(b, db, fmt.Sprintf("INSERT INTO users VALUES (%d, '%s', %d)", i, name, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM users WHERE username LIKE 'alice%'")
	}
}

// -----------------------------------------------------------------
// Wave 12 (v0.7.4): EXISTS Early Exit & Hash Join Pool
// -----------------------------------------------------------------

// BenchmarkExistsSubquery measures EXISTS subquery throughput.
// The LIMIT-1 short-circuit stops after the first matching row.
func BenchmarkExistsSubquery(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)")
	mustExec(b, db, "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER, val TEXT)")

	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO parent VALUES (%d, 'p%d')", i, i))
	}
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO child VALUES (%d, %d, 'v%d')", i, i%100, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM parent WHERE EXISTS (SELECT 1 FROM child WHERE child.parent_id = parent.id)")
	}
}

// BenchmarkHashJoinWithWhere measures hash join throughput when a WHERE clause
// requires building a merged-row map.  The sync.Pool reuse reduces allocations.
func BenchmarkHashJoinWithWhere(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE dept (id INTEGER PRIMARY KEY, name TEXT, budget INTEGER)")
	mustExec(b, db, "CREATE TABLE emp (id INTEGER PRIMARY KEY, dept_id INTEGER, salary INTEGER)")

	for i := 0; i < 20; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO dept VALUES (%d, 'd%d', %d)", i, i, (i+1)*10000))
	}
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO emp VALUES (%d, %d, %d)", i, i%20, 30000+i*100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT emp.id, dept.name FROM emp JOIN dept ON emp.dept_id = dept.id WHERE emp.salary > 40000")
	}
}
