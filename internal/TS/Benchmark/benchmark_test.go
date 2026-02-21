// Package Benchmark provides SQL-level performance benchmarks for sqlvibe.
// Each benchmark exercises the public SQL interface (pkg/sqlvibe) so that
// end-to-end query latency is measured, including parsing, code generation,
// and VM execution.
package Benchmark

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
)

// openDB opens an in-memory sqlvibe database for benchmarking.
func openDB(b *testing.B) *sqlvibe.Database {
	b.Helper()
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		b.Fatalf("Failed to open sqlvibe: %v", err)
	}
	return db
}

// mustExec runs a SQL statement and fails the benchmark on error.
func mustExec(b *testing.B, db *sqlvibe.Database, sql string) {
	b.Helper()
	if _, err := db.Exec(sql); err != nil {
		b.Fatalf("Exec(%q) failed: %v", sql, err)
	}
}

// mustQuery runs a SQL query, fetches all rows, and fails on error.
func mustQuery(b *testing.B, db *sqlvibe.Database, sql string) *sqlvibe.Rows {
	b.Helper()
	rows, err := db.Query(sql)
	if err != nil {
		b.Fatalf("Query(%q) failed: %v", sql, err)
	}
	return rows
}

// -----------------------------------------------------------------
// BenchmarkInsertSingle measures single-row INSERT throughput.
// -----------------------------------------------------------------
func BenchmarkInsertSingle(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'value-%d')", i, i))
	}
}

// -----------------------------------------------------------------
// BenchmarkInsertBatch measures throughput when inserting 100 rows
// per iteration via repeated single INSERTs (no multi-row syntax).
// -----------------------------------------------------------------
func BenchmarkInsertBatch100(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mustExec(b, db, "DELETE FROM t")
		b.StartTimer()
		for j := 0; j < 100; j++ {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'v%d')", j, j))
		}
	}
}

// -----------------------------------------------------------------
// BenchmarkSelectAll measures SELECT * throughput on a 1000-row table.
// -----------------------------------------------------------------
func BenchmarkSelectAll(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, name TEXT, score REAL)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'name%d', %f)", i, i, float64(i)*1.1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t")
	}
}

// -----------------------------------------------------------------
// BenchmarkSelectWhere measures filtered SELECT with a WHERE clause.
// -----------------------------------------------------------------
func BenchmarkSelectWhere(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE val = 5")
	}
}

// -----------------------------------------------------------------
// BenchmarkSelectOrderBy measures SELECT with ORDER BY.
// -----------------------------------------------------------------
func BenchmarkSelectOrderBy(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, score INTEGER)")
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, 500-i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t ORDER BY score ASC")
	}
}

// -----------------------------------------------------------------
// BenchmarkSelectAggregate measures COUNT/SUM/AVG/MIN/MAX aggregate queries.
// -----------------------------------------------------------------
func BenchmarkSelectAggregate(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 1; i <= 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	cases := []struct {
		name string
		sql  string
	}{
		{"COUNT", "SELECT COUNT(*) FROM t"},
		{"SUM", "SELECT SUM(val) FROM t"},
		{"AVG", "SELECT AVG(val) FROM t"},
		{"MIN", "SELECT MIN(val) FROM t"},
		{"MAX", "SELECT MAX(val) FROM t"},
		{"CountWhere", "SELECT COUNT(*) FROM t WHERE val > 500"},
	}

	for _, tc := range cases {
		tc := tc
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				mustQuery(b, db, tc.sql)
			}
		})
	}
}

// -----------------------------------------------------------------
// BenchmarkSelectGroupBy measures GROUP BY aggregation.
// -----------------------------------------------------------------
func BenchmarkSelectGroupBy(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE sales (region TEXT, amount INTEGER)")
	regions := []string{"North", "South", "East", "West"}
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO sales VALUES ('%s', %d)", regions[i%4], i+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT region, SUM(amount), COUNT(*), AVG(amount) FROM sales GROUP BY region ORDER BY region")
	}
}

// -----------------------------------------------------------------
// BenchmarkSelectJoin measures INNER JOIN across two tables.
// -----------------------------------------------------------------
func BenchmarkSelectJoin(b *testing.B) {
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
		mustQuery(b, db, "SELECT u.name, o.amount FROM users AS u JOIN orders AS o ON u.id = o.user_id WHERE o.amount > 1000")
	}
}

// -----------------------------------------------------------------
// BenchmarkSelectSubquery measures correlated and non-correlated subqueries.
// -----------------------------------------------------------------
func BenchmarkSelectSubquery(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER, category INTEGER)")
	for i := 1; i <= 200; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d)", i, i, i%5))
	}

	b.Run("InSubquery", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mustQuery(b, db, "SELECT * FROM t WHERE id IN (SELECT id FROM t WHERE category = 2)")
		}
	})

	b.Run("ScalarSubquery", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mustQuery(b, db, "SELECT * FROM t WHERE val > (SELECT AVG(val) FROM t)")
		}
	})
}

// -----------------------------------------------------------------
// BenchmarkUpdate measures UPDATE throughput.
// -----------------------------------------------------------------
func BenchmarkUpdate(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 1; i <= 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExec(b, db, fmt.Sprintf("UPDATE t SET val = %d WHERE id = %d", i, (i%100)+1))
	}
}

// -----------------------------------------------------------------
// BenchmarkDelete measures DELETE throughput with re-seeding.
// -----------------------------------------------------------------
func BenchmarkDelete(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for j := 0; j < 10; j++ {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i*10+j, j))
		}
		b.StartTimer()
		mustExec(b, db, fmt.Sprintf("DELETE FROM t WHERE id >= %d AND id < %d", i*10, i*10+10))
	}
}

// -----------------------------------------------------------------
// BenchmarkSelectLike measures LIKE pattern matching.
// -----------------------------------------------------------------
func BenchmarkSelectLike(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, name TEXT)")
	prefixes := []string{"Alice", "Bob", "Carol", "Dave", "Eve"}
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, '%s_%d')", i, prefixes[i%5], i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE name LIKE 'Alice%'")
	}
}

// -----------------------------------------------------------------
// BenchmarkSelectUnion measures UNION ALL on two tables.
// -----------------------------------------------------------------
func BenchmarkSelectUnion(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE a (id INTEGER, val TEXT)")
	mustExec(b, db, "CREATE TABLE b (id INTEGER, val TEXT)")
	for i := 0; i < 200; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO a VALUES (%d, 'a%d')", i, i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO b VALUES (%d, 'b%d')", i, i))
	}

	b.Run("UnionAll", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mustQuery(b, db, "SELECT id, val FROM a UNION ALL SELECT id, val FROM b")
		}
	})

	b.Run("Union", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mustQuery(b, db, "SELECT id, val FROM a UNION SELECT id, val FROM b")
		}
	})
}

// -----------------------------------------------------------------
// BenchmarkDDL measures CREATE TABLE / DROP TABLE throughput.
// -----------------------------------------------------------------
func BenchmarkDDL(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("tbl_%d", i)
		mustExec(b, db, fmt.Sprintf("CREATE TABLE %s (id INTEGER, val TEXT)", name))
		mustExec(b, db, fmt.Sprintf("DROP TABLE %s", name))
	}
}

// -----------------------------------------------------------------
// BenchmarkCASE measures CASE expression evaluation.
// -----------------------------------------------------------------
func BenchmarkCASE(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, score INTEGER)")
	for i := 0; i < 300; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, `SELECT id,
			CASE
				WHEN score >= 90 THEN 'A'
				WHEN score >= 80 THEN 'B'
				WHEN score >= 70 THEN 'C'
				ELSE 'F'
			END AS grade
		FROM t`)
	}
}

// -----------------------------------------------------------------
// BenchmarkSelectLimit measures SELECT with LIMIT/OFFSET.
// -----------------------------------------------------------------
func BenchmarkSelectLimit(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.Run("Limit10", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mustQuery(b, db, "SELECT * FROM t ORDER BY id LIMIT 10")
		}
	})

	b.Run("Limit10Offset100", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mustQuery(b, db, "SELECT * FROM t ORDER BY id LIMIT 10 OFFSET 100")
		}
	})
}

// -----------------------------------------------------------------
// BenchmarkSchemaCreation measures creating a realistic schema once
// (CREATE TABLE + indexes) — useful for cold-start cost.
// -----------------------------------------------------------------
func BenchmarkSchemaCreation(b *testing.B) {
	schema := strings.TrimSpace(`
CREATE TABLE employees (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	department TEXT,
	salary REAL,
	hire_date TEXT
);
CREATE TABLE departments (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL UNIQUE,
	budget REAL
);
CREATE TABLE projects (
	id INTEGER PRIMARY KEY,
	name TEXT NOT NULL,
	department_id INTEGER,
	budget REAL
);
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db := openDB(b)
		for _, stmt := range strings.Split(schema, ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			mustExec(b, db, stmt)
		}
		db.Close()
	}
}

// -----------------------------------------------------------------
// Heavy SQL Benchmarks - Complex Queries for Bottleneck Discovery
// -----------------------------------------------------------------

// BenchmarkHeavySubqueryExists measures EXISTS subquery performance
func BenchmarkHeavySubqueryExists(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER)")
	mustExec(b, db, "CREATE TABLE customers (id INTEGER, name TEXT)")
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO customers VALUES (%d, 'cust%d')", i, i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %d)", i, i%500, i*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 1000)")
	}
}

// BenchmarkHeavyInClauseLarge measures large IN clause performance
func BenchmarkHeavyInClauseLarge(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	inList := "1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE id IN ("+inList+")")
	}
}

// BenchmarkHeavyStringConcat measures string concatenation performance
func BenchmarkHeavyStringConcat(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE users (id INTEGER, first_name TEXT, last_name TEXT)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO users VALUES (%d, 'FirstName%d', 'LastName%d')", i, i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT id, first_name || ' ' || last_name AS full_name FROM users")
	}
}

// BenchmarkHeavyMultipleAggregates measures multiple aggregates in one query
func BenchmarkHeavyMultipleAggregates(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE sales (id INTEGER, region TEXT, amount INTEGER, quantity INTEGER)")
	regions := []string{"North", "South", "East", "West"}
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO sales VALUES (%d, '%s', %d, %d)", i, regions[i%4], i*10, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT region, COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount), SUM(quantity), AVG(quantity) FROM sales GROUP BY region")
	}
}

// BenchmarkHeavyCorrelatedSubquery measures correlated subquery performance
func BenchmarkHeavyCorrelatedSubquery(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE employees (id INTEGER, dept_id INTEGER, salary INTEGER)")
	for i := 0; i < 200; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO employees VALUES (%d, %d, %d)", i, i%10, 30000+i*100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM employees e1 WHERE salary > (SELECT AVG(salary) FROM employees e2 WHERE e2.dept_id = e1.dept_id)")
	}
}

// BenchmarkHeavyComplexCase measures complex CASE with multiple conditions
func BenchmarkHeavyComplexCase(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE products (id INTEGER, category TEXT, price INTEGER, stock INTEGER)")
	categories := []string{"Electronics", "Clothing", "Food", "Books", "Sports"}
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO products VALUES (%d, '%s', %d, %d)", i, categories[i%5], (i%100)*10, i%50))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT id, category, price, stock, CASE WHEN category = 'Electronics' AND price > 500 THEN 'Premium' WHEN category = 'Electronics' THEN 'Basic' WHEN category = 'Clothing' AND stock > 25 THEN 'In Stock' WHEN category = 'Clothing' THEN 'Low Stock' WHEN category = 'Food' AND price > 50 THEN 'Premium Food' WHEN category = 'Food' THEN 'Regular Food' WHEN price > 800 THEN 'High Value' WHEN price < 100 THEN 'Budget' ELSE 'Standard' END AS classification FROM products")
	}
}

// BenchmarkHeavyHavingClause measures HAVING clause performance
func BenchmarkHeavyHavingClause(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE orders (id INTEGER, customer_id INTEGER, amount INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %d)", i, i%100, (i%50)*10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT customer_id, SUM(amount) as total FROM orders GROUP BY customer_id HAVING SUM(amount) > 1000")
	}
}

// BenchmarkHeavyDistinct measures DISTINCT performance
func BenchmarkHeavyDistinct(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, cat1 TEXT, cat2 TEXT, cat3 TEXT)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'A%d', 'B%d', 'C%d')", i, i%10, i%20, i%30))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT DISTINCT cat1, cat2, cat3 FROM t")
	}
}

// BenchmarkHeavyMultiTableLeftJoin measures multiple LEFT JOINs
func BenchmarkHeavyMultiTableLeftJoin(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE users (id INTEGER, name TEXT)")
	mustExec(b, db, "CREATE TABLE orders (id INTEGER, user_id INTEGER)")
	mustExec(b, db, "CREATE TABLE products (id INTEGER, name TEXT)")
	mustExec(b, db, "CREATE TABLE order_items (id INTEGER, order_id INTEGER, product_id INTEGER)")

	for i := 0; i < 50; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO users VALUES (%d, 'user%d')", i, i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d)", i, i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO products VALUES (%d, 'prod%d')", i, i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO order_items VALUES (%d, %d, %d)", i, i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT u.name, o.id, p.name FROM users u LEFT JOIN orders o ON u.id = o.user_id LEFT JOIN order_items oi ON o.id = oi.order_id LEFT JOIN products p ON oi.product_id = p.id")
	}
}

// BenchmarkHeavyScalarFunctions measures multiple scalar functions
func BenchmarkHeavyScalarFunctions(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val TEXT, num INTEGER)")
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'value_%d', %d)", i, i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT UPPER(val), LOWER(val), LENGTH(val), SUBSTR(val, 1, 5), ABS(num) FROM t")
	}
}

// BenchmarkHeavyBetweenAnd measures BETWEEN and AND combinations
func BenchmarkHeavyBetweenAnd(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val1 INTEGER, val2 INTEGER, val3 INTEGER)")
	for i := 0; i < 2000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d, %d)", i, i, i*2, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE val1 BETWEEN 100 AND 500 AND val2 BETWEEN 200 AND 1000 AND val3 IN (10, 20, 30)")
	}
}

// BenchmarkHeavyCoalesceNULL measures COALESCE with many NULLs
func BenchmarkHeavyCoalesceNULL(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, col1 TEXT, col2 TEXT, col3 TEXT, col4 TEXT)")
	for i := 0; i < 500; i++ {
		if i%3 == 0 {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'val%d', NULL, NULL, NULL)", i, i))
		} else if i%3 == 1 {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, NULL, 'val%d', NULL, NULL)", i, i))
		} else {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, NULL, NULL, 'val%d', NULL)", i, i))
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT id, COALESCE(col1, col2, col3, col4, 'N/A') as value FROM t")
	}
}

// BenchmarkHeavyNullCheck measures IS NULL / IS NOT NULL
func BenchmarkHeavyNullCheck(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 1000; i++ {
		if i%5 == 0 {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, NULL)", i))
		} else {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE val IS NOT NULL")
	}
}

// BenchmarkHeavyOrderByMultiple measures ORDER BY multiple columns
func BenchmarkHeavyOrderByMultiple(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, cat TEXT, val INTEGER)")
	categories := []string{"A", "B", "C", "D"}
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, '%s', %d)", i, categories[i%4], 1000-i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t ORDER BY cat ASC, val DESC, id ASC")
	}
}

// BenchmarkHeavyTableScanFull measures full table scan without index
func BenchmarkHeavyTableScanFull(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 5000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE val = 50")
	}
}

// BenchmarkHeavyComplexWhere measures complex WHERE with functions
func BenchmarkHeavyComplexWhere(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE events (id INTEGER, name TEXT, timestamp TEXT, severity INTEGER)")
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO events VALUES (%d, 'event%d', '2024-01-%02d', %d)", i, i, i%28+1, i%5+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM events WHERE severity > 2 AND LENGTH(name) > 5 AND timestamp > '2024-01-15'")
	}
}

// BenchmarkHeavyRecursivePattern measures recursive-like pattern
func BenchmarkHeavyRecursivePattern(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE org_chart (employee_id INTEGER, manager_id INTEGER, name TEXT, level INTEGER)")
	for i := 1; i <= 100; i++ {
		managerID := 1
		if i > 1 {
			managerID = (i - 1) / 2
		}
		mustExec(b, db, fmt.Sprintf("INSERT INTO org_chart VALUES (%d, %d, 'emp%d', %d)", i, managerID, i, 0))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM org_chart WHERE manager_id IN (SELECT employee_id FROM org_chart WHERE manager_id IN (1, 2))")
	}
}

// BenchmarkHeavyInsertTransaction measures INSERT in transaction
func BenchmarkHeavyInsertTransaction(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin()
		for j := 0; j < 50; j++ {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i*50+j, j))
		}
		tx.Commit()
	}
}

// BenchmarkHeavyUpdateJoin measures UPDATE with JOIN
func BenchmarkHeavyUpdateJoin(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE products (id INTEGER, price INTEGER)")
	mustExec(b, db, "CREATE TABLE discounts (product_id INTEGER, discount INTEGER)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO products VALUES (%d, %d)", i, 100+i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO discounts VALUES (%d, %d)", i, i%20+5))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT p.id, p.price, d.discount, p.price - d.discount as final_price FROM products p JOIN discounts d ON p.id = d.product_id")
	}
}

// BenchmarkHeavyAutoIncrement measures auto-increment insert
func BenchmarkHeavyAutoIncrement(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, val TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExec(b, db, "INSERT INTO t (val) VALUES ('value')")
	}
}
