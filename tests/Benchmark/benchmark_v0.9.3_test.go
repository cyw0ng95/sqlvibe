// Package Benchmark provides v0.9.3 performance benchmarks.
// These benchmarks cover the v0.9.3 optimizations:
//   - Extended dispatch table (comparison + string opcodes)
//   - SIMD-style vectorized batch operations (via SQL aggregation in v0.11.2+)
//   - INSERT OR REPLACE / INSERT OR IGNORE throughput
package Benchmark

import (
	"fmt"
	"testing"
)

// -----------------------------------------------------------------
// Extended Dispatch: comparison opcodes
// -----------------------------------------------------------------

// BenchmarkDispatchComparison measures SELECT with WHERE comparisons, which
// exercise the new comparison opcodes in the dispatch table.
func BenchmarkDispatchComparison(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*2))
	}

	b.Run("WhereEq", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.ClearResultCache()
			b.StartTimer()
			rows := mustQuery(b, db, "SELECT id FROM t WHERE val = 500")
			for rows.Next() {
			}
		}
	})

	b.Run("WhereLt", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.ClearResultCache()
			b.StartTimer()
			rows := mustQuery(b, db, "SELECT id FROM t WHERE val < 200")
			for rows.Next() {
			}
		}
	})

	b.Run("WhereGe", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.ClearResultCache()
			b.StartTimer()
			rows := mustQuery(b, db, "SELECT id FROM t WHERE val >= 900")
			for rows.Next() {
			}
		}
	})
}

// -----------------------------------------------------------------
// Extended Dispatch: string opcodes
// -----------------------------------------------------------------

// BenchmarkDispatchStringOps measures string function opcodes in the dispatch table.
func BenchmarkDispatchStringOps(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE words (id INTEGER, word TEXT)")
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO words VALUES (%d, '  hello world  ')", i))
	}

	b.Run("Trim", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.ClearResultCache()
			b.StartTimer()
			rows := mustQuery(b, db, "SELECT TRIM(word) FROM words")
			for rows.Next() {
			}
		}
	})

	b.Run("Replace", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.ClearResultCache()
			b.StartTimer()
			rows := mustQuery(b, db, "SELECT REPLACE(word, 'world', 'there') FROM words")
			for rows.Next() {
			}
		}
	})
}

// -----------------------------------------------------------------
// Vectorized batch operations (v0.9.3)
// In v0.11.2+ these are handled by the C++ engine automatically.
// The benchmarks measure SQL aggregation/expression throughput.
// -----------------------------------------------------------------

// BenchmarkSIMDVectorSumInt64 measures SQL SUM throughput on integer columns.
func BenchmarkSIMDVectorSumInt64(b *testing.B) {
	sizes := []int{256, 1024, 4096}
	for _, n := range sizes {
		n := n
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db := openDB(b)
			defer db.Close()
			mustExec(b, db, "CREATE TABLE t (val INTEGER)")
			for i := 0; i < n; i++ {
				mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db.ClearResultCache()
				b.StartTimer()
				rows := mustQuery(b, db, "SELECT SUM(val) FROM t")
				for rows.Next() {
				}
			}
		})
	}
}

// BenchmarkSIMDVectorSumFloat64 measures SQL SUM throughput on float columns.
func BenchmarkSIMDVectorSumFloat64(b *testing.B) {
	sizes := []int{256, 1024, 4096}
	for _, n := range sizes {
		n := n
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			db := openDB(b)
			defer db.Close()
			mustExec(b, db, "CREATE TABLE t (val REAL)")
			for i := 0; i < n; i++ {
				mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%f)", float64(i)*1.5))
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				db.ClearResultCache()
				b.StartTimer()
				rows := mustQuery(b, db, "SELECT SUM(val) FROM t")
				for rows.Next() {
				}
			}
		})
	}
}

// BenchmarkSIMDVectorAddInt64 measures SQL element-wise integer addition throughput.
func BenchmarkSIMDVectorAddInt64(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (a INTEGER, v INTEGER)")
	for i := 0; i < 1024; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i*2))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT a + v FROM t")
		for rows.Next() {
		}
	}
}

// BenchmarkSIMDVectorMulFloat64 measures SQL element-wise float multiplication throughput.
func BenchmarkSIMDVectorMulFloat64(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (a REAL, v REAL)")
	for i := 0; i < 1024; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%f, %f)", float64(i), float64(i)*1.5))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT a * v FROM t")
		for rows.Next() {
		}
	}
}

// -----------------------------------------------------------------
// INSERT OR REPLACE / IGNORE throughput
// -----------------------------------------------------------------

// BenchmarkInsertOrReplace measures throughput of INSERT OR REPLACE.
func BenchmarkInsertOrReplace(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE kv (k INTEGER PRIMARY KEY, v TEXT)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO kv VALUES (%d, 'init')", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := i % 100
		mustExec(b, db, fmt.Sprintf("INSERT OR REPLACE INTO kv VALUES (%d, 'updated')", key))
	}
}

// BenchmarkInsertOrIgnore measures throughput of INSERT OR IGNORE.
func BenchmarkInsertOrIgnore(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE kv2 (k INTEGER PRIMARY KEY, v TEXT)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO kv2 VALUES (%d, 'init')", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := i % 100
		mustExec(b, db, fmt.Sprintf("INSERT OR IGNORE INTO kv2 VALUES (%d, 'dup')", key))
	}
}
