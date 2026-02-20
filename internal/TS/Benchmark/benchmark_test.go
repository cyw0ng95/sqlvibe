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
