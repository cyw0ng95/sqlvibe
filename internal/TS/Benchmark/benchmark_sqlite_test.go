package Benchmark

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/glebarez/go-sqlite"
)

func openSQLiteDB(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatalf("Failed to open SQLite: %v", err)
	}
	return db
}

func mustExecSQLite(b *testing.B, db *sql.DB, sql string) {
	b.Helper()
	if _, err := db.Exec(sql); err != nil {
		b.Fatalf("SQLite Exec(%q) failed: %v", sql, err)
	}
}

func mustQuerySQLite(b *testing.B, db *sql.DB, sql string) *sql.Rows {
	b.Helper()
	rows, err := db.Query(sql)
	if err != nil {
		b.Fatalf("SQLite Query(%q) failed: %v", sql, err)
	}
	return rows
}

func BenchmarkSQLiteInsertSingle(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'value-%d')", i, i))
	}
}

func BenchmarkSQLiteInsertBatch100(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER, val TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mustExecSQLite(b, db, "DELETE FROM t")
		b.StartTimer()
		for j := 0; j < 100; j++ {
			mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'v%d')", j, j))
		}
	}
}

func BenchmarkSQLiteSelectAll(b *testing.B) {
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

func BenchmarkSQLiteSelectWhere(b *testing.B) {
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

func BenchmarkSQLiteSelectOrderBy(b *testing.B) {
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

func BenchmarkSQLiteSelectAggregateCOUNT(b *testing.B) {
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

func BenchmarkSQLiteSelectAggregateSUM(b *testing.B) {
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

func BenchmarkSQLiteGroupBy(b *testing.B) {
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

func BenchmarkSQLiteJoin(b *testing.B) {
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

func BenchmarkSQLiteSubqueryIn(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER, val INTEGER, category INTEGER)")
	for i := 1; i <= 200; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d)", i, i, i%5))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT * FROM t WHERE id IN (SELECT id FROM t WHERE category = 2)")
		for rows.Next() {
		}
		rows.Close()
	}
}

func BenchmarkSQLiteSubqueryScalar(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER, val INTEGER, category INTEGER)")
	for i := 1; i <= 200; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d)", i, i, i%5))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT * FROM t WHERE val > (SELECT AVG(val) FROM t)")
		for rows.Next() {
		}
		rows.Close()
	}
}

func BenchmarkSQLiteUpdate(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 1; i <= 100; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("UPDATE t SET val = %d WHERE id = %d", i, (i%100)+1))
	}
}

func BenchmarkSQLiteDelete(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for j := 0; j < 10; j++ {
			mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i*10+j, j))
		}
		b.StartTimer()
		mustExecSQLite(b, db, fmt.Sprintf("DELETE FROM t WHERE id >= %d AND id < %d", i*10, i*10+10))
	}
}

func BenchmarkSQLiteLike(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER, name TEXT)")
	prefixes := []string{"Alice", "Bob", "Carol", "Dave", "Eve"}
	for i := 0; i < 500; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, '%s_%d')", i, prefixes[i%5], i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT * FROM t WHERE name LIKE 'Alice%'")
		for rows.Next() {
		}
		rows.Close()
	}
}

func BenchmarkSQLiteUnionAll(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE a (id INTEGER, val TEXT)")
	mustExecSQLite(b, db, "CREATE TABLE b (id INTEGER, val TEXT)")
	for i := 0; i < 200; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO a VALUES (%d, 'a%d')", i, i))
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO b VALUES (%d, 'b%d')", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT id, val FROM a UNION ALL SELECT id, val FROM b")
		for rows.Next() {
		}
		rows.Close()
	}
}

func BenchmarkSQLiteCASE(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER, score INTEGER)")
	for i := 0; i < 300; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%100))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, `SELECT id, CASE WHEN score >= 90 THEN 'A' WHEN score >= 80 THEN 'B' WHEN score >= 70 THEN 'C' ELSE 'F' END AS grade FROM t`)
		for rows.Next() {
		}
		rows.Close()
	}
}

func BenchmarkSQLiteLimit10(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := mustQuerySQLite(b, db, "SELECT * FROM t ORDER BY id LIMIT 10")
		for rows.Next() {
		}
		rows.Close()
	}
}
