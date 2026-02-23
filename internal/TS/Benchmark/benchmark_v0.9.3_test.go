// Package Benchmark provides v0.9.3 performance benchmarks.
// These benchmarks cover the v0.9.3 optimizations:
//   - Extended dispatch table (comparison + string opcodes)
//   - SIMD-style vectorized batch operations (int64/float64)
//   - INSERT OR REPLACE / INSERT OR IGNORE throughput
package Benchmark

import (
	"fmt"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
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
// SIMD Vectorization: batch int64 sum
// -----------------------------------------------------------------

// BenchmarkSIMDVectorSumInt64 measures the vectorized int64 sum.
func BenchmarkSIMDVectorSumInt64(b *testing.B) {
	sizes := []int{256, 1024, 4096}
	for _, n := range sizes {
		n := n
		data := make([]int64, n)
		for i := range data {
			data[i] = int64(i)
		}
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = sqlvibe.VectorSumInt64(data)
			}
		})
	}
}

// BenchmarkSIMDVectorSumFloat64 measures the vectorized float64 sum.
func BenchmarkSIMDVectorSumFloat64(b *testing.B) {
	sizes := []int{256, 1024, 4096}
	for _, n := range sizes {
		n := n
		data := make([]float64, n)
		for i := range data {
			data[i] = float64(i) * 1.5
		}
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = sqlvibe.VectorSumFloat64(data)
			}
		})
	}
}

// BenchmarkSIMDVectorAddInt64 measures vectorized int64 element-wise add.
func BenchmarkSIMDVectorAddInt64(b *testing.B) {
	n := 1024
	a := make([]int64, n)
	bv := make([]int64, n)
	dst := make([]int64, n)
	for i := range a {
		a[i] = int64(i)
		bv[i] = int64(i * 2)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqlvibe.VectorAddInt64(dst, a, bv)
	}
}

// BenchmarkSIMDVectorMulFloat64 measures vectorized float64 element-wise multiply.
func BenchmarkSIMDVectorMulFloat64(b *testing.B) {
	n := 1024
	a := make([]float64, n)
	bv := make([]float64, n)
	dst := make([]float64, n)
	for i := range a {
		a[i] = float64(i)
		bv[i] = float64(i) * 1.5
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqlvibe.VectorMulFloat64(dst, a, bv)
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
