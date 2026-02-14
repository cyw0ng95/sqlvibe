package sqlvibe

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/sqlvibe/sqlvibe/internal/qp"
)

func BenchmarkCreateTable(b *testing.B) {
	path := "/tmp/bench_create.db"
	os.Remove(path)
	defer os.Remove(path)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		os.Remove(path)
		db, _ := Open(path)
		db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
		db.Close()
	}
}

func BenchmarkInsert(b *testing.B) {
	path := "/tmp/bench_insert.db"
	os.Remove(path)
	defer os.Remove(path)

	db, _ := Open(path)
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO test VALUES (%d, 'value%d')", i, i))
	}

	b.StopTimer()
	db.Close()
}

func BenchmarkSelectAll(b *testing.B) {
	path := "/tmp/bench_select.db"
	os.Remove(path)
	defer os.Remove(path)

	db, _ := Open(path)
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	for i := 0; i < 1000; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO test VALUES (%d, 'value%d')", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Query("SELECT * FROM test")
	}

	b.StopTimer()
	db.Close()
}

func BenchmarkSelectWhere(b *testing.B) {
	path := "/tmp/bench_select_where.db"
	os.Remove(path)
	defer os.Remove(path)

	db, _ := Open(path)
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	for i := 0; i < 1000; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO test VALUES (%d, 'value%d')", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Query("SELECT * FROM test WHERE id = 500")
	}

	b.StopTimer()
	db.Close()
}

func BenchmarkUpdate(b *testing.B) {
	path := "/tmp/bench_update.db"
	os.Remove(path)
	defer os.Remove(path)

	db, _ := Open(path)
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value INTEGER)")
	for i := 0; i < 1000; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO test VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec("UPDATE test SET value = value + 1 WHERE id < 100")
	}

	b.StopTimer()
	db.Close()
}

func BenchmarkDelete(b *testing.B) {
	path := "/tmp/bench_delete.db"
	os.Remove(path)
	defer os.Remove(path)

	db, _ := Open(path)
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
	for i := 0; i < 1000; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO test VALUES (%d, 'value%d')", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec("DELETE FROM test WHERE id < 10")
	}

	b.StopTimer()
	db.Close()
}

func BenchmarkTransactionCommit(b *testing.B) {
	path := "/tmp/bench_tx.db"
	os.Remove(path)
	defer os.Remove(path)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		os.Remove(path)
		db, _ := Open(path)
		db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")

		tx, _ := db.Begin()
		tx.Exec("INSERT INTO test VALUES (1, 'one')")
		tx.Commit()

		db.Close()
	}
}

func BenchmarkTokenize(b *testing.B) {
	sql := "SELECT id, name, age FROM users WHERE age > 25 ORDER BY name LIMIT 10"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tokenizer := qp.NewTokenizer(sql)
		tokenizer.Tokenize()
	}
}

func BenchmarkParse(b *testing.B) {
	sql := "SELECT id, name, age FROM users WHERE age > 25 ORDER BY name LIMIT 10"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tokenizer := qp.NewTokenizer(sql)
		tokens, _ := tokenizer.Tokenize()
		parser := qp.NewParser(tokens)
		parser.Parse()
	}
}

func BenchmarkFileOpen(b *testing.B) {
	path := "/tmp/bench_file.db"
	os.Remove(path)
	defer os.Remove(path)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		os.Remove(path)
		db, _ := Open(path)
		db.Close()
	}
}

func BenchmarkFileWrite(b *testing.B) {
	path := "/tmp/bench_file_write.db"
	os.Remove(path)
	defer os.Remove(path)

	db, _ := Open(path)
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO test VALUES (%d)", i))
	}

	b.StopTimer()
	db.Close()
}

func BenchmarkThroughput(b *testing.B) {
	path := "/tmp/bench_throughput.db"
	os.Remove(path)
	defer os.Remove(path)

	db, _ := Open(path)
	db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")

	start := time.Now()
	for i := 0; i < 10000; i++ {
		db.Exec(fmt.Sprintf("INSERT INTO test VALUES (%d, 'value%d')", i, i))
	}
	elapsed := time.Since(start)

	b.ReportMetric(float64(10000)/elapsed.Seconds(), "ops/sec")
	db.Close()
}
