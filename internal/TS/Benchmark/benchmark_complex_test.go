// Package Benchmark provides complex SQL performance benchmarks for sqlvibe.
// These benchmarks test large datasets and complex queries to compare with SQLite.
package Benchmark

import (
	"fmt"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// -----------------------------------------------------------------
// LARGE TABLE BENCHMARKS (100K rows)
// -----------------------------------------------------------------

// BenchmarkLargeTable_SelectAll_100K measures full table scan on 100K rows
func BenchmarkLargeTable_SelectAll_100K(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER, name TEXT, amount REAL)")
	for i := 0; i < 100000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, 'name-%d', %.2f)", i, i%1000, i, float64(i)*1.5))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t")
	}
}

// BenchmarkLargeTable_CountStar_100K measures COUNT(*) on 100K rows
func BenchmarkLargeTable_CountStar_100K(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 0; i < 100000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

// BenchmarkLargeTable_Sum_100K measures SUM on 100K rows
func BenchmarkLargeTable_Sum_100K(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER)")
	for i := 0; i < 100000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%1000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT SUM(amount) FROM t")
	}
}

// BenchmarkLargeTable_Avg_100K measures AVG on 100K rows
func BenchmarkLargeTable_Avg_100K(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, amount INTEGER)")
	for i := 0; i < 100000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%1000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT AVG(amount) FROM t")
	}
}

// -----------------------------------------------------------------
// COMPLEX WHERE CLAUSES
// -----------------------------------------------------------------

// BenchmarkComplexWhere_MultiAnd measures multiple AND conditions
func BenchmarkComplexWhere_MultiAnd(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, a INTEGER, b INTEGER, c INTEGER, d TEXT)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d, %d, 'text-%d')", i, i%100, i%50, i%25, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE a > 50 AND b < 25 AND c >= 10")
	}
}

// BenchmarkComplexWhere_OrOr measures OR conditions
func BenchmarkComplexWhere_OrOr(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, status TEXT, type INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'status-%d', %d)", i, i%5, i%10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE status = 'status-1' OR status = 'status-2' OR type = 5")
	}
}

// BenchmarkComplexWhere_InClause measures IN clause with many values
func BenchmarkComplexWhere_InClause(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, category INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE category IN (1,5,10,15,20,25,30,35,40,45)")
	}
}

// BenchmarkComplexWhere_Between measures BETWEEN clause
func BenchmarkComplexWhere_Between(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, value INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE value BETWEEN 1000 AND 5000")
	}
}

// -----------------------------------------------------------------
// GROUP BY & AGGREGATES
// -----------------------------------------------------------------

// BenchmarkGroupBy_MultiColumn measures GROUP BY on multiple columns
func BenchmarkGroupBy_MultiColumn(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, region TEXT, category TEXT, amount INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'region-%d', 'cat-%d', %d)",
			i, i%10, i%20, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT region, category, SUM(amount), COUNT(*) FROM t GROUP BY region, category")
	}
}

// BenchmarkGroupBy_Having measures GROUP BY with HAVING
func BenchmarkGroupBy_Having(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, dept TEXT, salary INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'dept-%d', %d)", i, i%50, 30000+i%20000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT dept, AVG(salary) as avg_sal FROM t GROUP BY dept HAVING AVG(salary) > 40000")
	}
}

// BenchmarkAggregates_Multiple measures multiple aggregates in one query
func BenchmarkAggregates_Multiple(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, amount INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%1000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM t")
	}
}

// -----------------------------------------------------------------
// COMPLEX JOINS
// -----------------------------------------------------------------

// BenchmarkJoin_ThreeTable measures 3-table JOIN
func BenchmarkJoin_ThreeTable(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE orders (id INTEGER, customer_id INTEGER, product_id INTEGER, amount INTEGER)")
	mustExec(b, db, "CREATE TABLE customers (id INTEGER, name TEXT, city TEXT)")
	mustExec(b, db, "CREATE TABLE products (id INTEGER, name TEXT, category TEXT)")

	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %d, %d)", i, i%500, i%300, i%1000))
		mustExec(b, db, fmt.Sprintf("INSERT INTO customers VALUES (%d, 'customer-%d', 'city-%d')", i, i, i%20))
		mustExec(b, db, fmt.Sprintf("INSERT INTO products VALUES (%d, 'product-%d', 'cat-%d')", i, i, i%30))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, `
			SELECT o.id, c.name, p.name, o.amount 
			FROM orders o
			JOIN customers c ON o.customer_id = c.id
			JOIN products p ON o.product_id = p.id
		`)
	}
}

// BenchmarkJoin_SelfJoin measures self-join
func BenchmarkJoin_SelfJoin(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE employees (id INTEGER, name TEXT, manager_id INTEGER, salary INTEGER)")
	for i := 0; i < 1000; i++ {
		managerID := 0
		if i > 0 {
			managerID = (i - 1) / 10
		}
		mustExec(b, db, fmt.Sprintf("INSERT INTO employees VALUES (%d, 'emp-%d', %d, %d)", i, i, managerID, 50000+i%10000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, `
			SELECT e.name as employee, m.name as manager, e.salary
			FROM employees e
			LEFT JOIN employees m ON e.manager_id = m.id
		`)
	}
}

// -----------------------------------------------------------------
// SUBQUERIES
// -----------------------------------------------------------------

// BenchmarkSubquery_Correlated measures correlated subquery
func BenchmarkSubquery_Correlated(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE employees (id INTEGER, dept_id INTEGER, salary INTEGER)")
	mustExec(b, db, "CREATE TABLE departments (id INTEGER, name TEXT)")

	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO employees VALUES (%d, %d, %d)", i, i%50, 30000+i%20000))
		mustExec(b, db, fmt.Sprintf("INSERT INTO departments VALUES (%d, 'dept-%d')", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, `
			SELECT e.id, e.salary, d.name
			FROM employees e
			JOIN departments d ON e.dept_id = d.id
			WHERE e.salary > (SELECT AVG(salary) FROM employees WHERE dept_id = e.dept_id)
		`)
	}
}

// BenchmarkSubquery_Exists measures EXISTS subquery
func BenchmarkSubquery_Exists(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE orders (id INTEGER, customer_id INTEGER, total INTEGER)")
	mustExec(b, db, "CREATE TABLE customers (id INTEGER, name TEXT)")

	for i := 0; i < 5000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, %d)", i, i%1000, i%500))
		mustExec(b, db, fmt.Sprintf("INSERT INTO customers VALUES (%d, 'customer-%d')", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, `
			SELECT c.id, c.name
			FROM customers c
			WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.total > 2000)
		`)
	}
}

// BenchmarkSubquery_Scalar measures scalar subquery
func BenchmarkSubquery_Scalar(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE products (id INTEGER, category_id INTEGER, price INTEGER)")
	mustExec(b, db, "CREATE TABLE categories (id INTEGER, name TEXT)")

	for i := 0; i < 5000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO products VALUES (%d, %d, %d)", i, i%100, 10+i%500))
		mustExec(b, db, fmt.Sprintf("INSERT INTO categories VALUES (%d, 'cat-%d')", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, `
			SELECT p.id, p.price, (SELECT name FROM categories WHERE id = p.category_id) as category
			FROM products p
		`)
	}
}

// -----------------------------------------------------------------
// WINDOW FUNCTIONS
// -----------------------------------------------------------------

// BenchmarkWindow_RowNumber measures ROW_NUMBER() window function
func BenchmarkWindow_RowNumber(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, dept TEXT, salary INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'dept-%d', %d)", i, i%20, 30000+i%20000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT id, dept, salary, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) as rn FROM t")
	}
}

// BenchmarkWindow_Rank measures RANK() window function
func BenchmarkWindow_Rank(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, score INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT id, score, RANK() OVER (ORDER BY score DESC) as rk FROM t")
	}
}

// BenchmarkWindow_LagLead measures LAG/LEAD window functions
func BenchmarkWindow_LagLead(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, value INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT id, value, LAG(value, 1) OVER (ORDER BY id) as prev, LEAD(value, 1) OVER (ORDER BY id) as next FROM t")
	}
}

// -----------------------------------------------------------------
// LARGE TEXT/BLOB DATA
// -----------------------------------------------------------------

// BenchmarkLargeText_Substr measures SUBSTR on large TEXT
func BenchmarkLargeText_Substr(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, text TEXT)")
	for i := 0; i < 1000; i++ {
		text := fmt.Sprintf("This is a long text field with id %d and some padding data to make it realistic", i)
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, '%s')", i, text))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT id, SUBSTR(text, 1, 20) FROM t")
	}
}

// -----------------------------------------------------------------
// ORDER BY & LIMIT
// -----------------------------------------------------------------

// BenchmarkOrderBy_LargeLimit measures ORDER BY with large LIMIT
func BenchmarkOrderBy_LargeLimit(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, value INTEGER)")
	for i := 0; i < 50000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%1000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t ORDER BY value DESC LIMIT 1000")
	}
}

// BenchmarkOrderBy_MultiColumn measures ORDER BY on multiple columns
func BenchmarkOrderBy_MultiColumn(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, a INTEGER, b INTEGER, c INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d, %d)", i, i%10, i%100, i%1000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t ORDER BY a, b, c")
	}
}

// -----------------------------------------------------------------
// UPDATE & DELETE
// -----------------------------------------------------------------

// BenchmarkUpdate_LargeTable measures UPDATE on large table
func BenchmarkUpdate_LargeTable(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, value INTEGER)")
	for i := 0; i < 50000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExec(b, db, "UPDATE t SET value = value + 1 WHERE value > 50")
	}
}

// BenchmarkDelete_LargeTable measures DELETE on large table
func BenchmarkDelete_LargeTable(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, value INTEGER)")
	for i := 0; i < 50000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExec(b, db, "DELETE FROM t WHERE value < 10")
	}
}

// -----------------------------------------------------------------
// DISTINCT & UNION
// -----------------------------------------------------------------

// BenchmarkDistinct_Large measures DISTINCT on large dataset
func BenchmarkDistinct_Large(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, category TEXT, region TEXT)")
	for i := 0; i < 100000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'cat-%d', 'region-%d')", i, i%100, i%50))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT DISTINCT category, region FROM t")
	}
}

// BenchmarkUnion_All measures UNION ALL
func BenchmarkUnion_All(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t1 (id INTEGER, value INTEGER)")
	mustExec(b, db, "CREATE TABLE t2 (id INTEGER, value INTEGER)")

	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t1 VALUES (%d, %d)", i, i%100))
		mustExec(b, db, fmt.Sprintf("INSERT INTO t2 VALUES (%d, %d)", i+10000, (i+100)%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t1 UNION ALL SELECT * FROM t2")
	}
}
