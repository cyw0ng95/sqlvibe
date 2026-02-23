// Package Benchmark provides v0.9.1 additional performance benchmarks.
// These benchmarks cover the v0.9.1 additional optimizations:
//   - Early termination for LIMIT (stops VM after N rows are collected)
//   - AND index lookup (uses index on one sub-predicate of a compound AND)
//   - Pre-sized result slices (reduces allocations in column-name building)
//   - Prepared statement pool (LRU-evicting cache of compiled query plans)
//   - Slab allocator (bump-pointer slab with sync.Pool for small objects)
//   - Expression bytecode (stack-machine evaluator for SQL expressions)
//   - Direct compiler fast-path detection (simple SELECT classification)
//
// NOTE on cache fairness: sqlvibe has an in-process result cache keyed on the
// SQL string. The benchmarks call db.ClearResultCache() before each iteration
// via b.StopTimer()/b.StartTimer() so actual query-execution cost is measured,
// not cache-hit cost. The plan cache (bytecode compilation) is intentionally
// kept warm, matching how database/sql keeps SQLite prepared statements alive
// across iterations. This gives an apples-to-apples comparison of per-query
// execution time for both engines.
package Benchmark

import (
	"fmt"
	"testing"

	DS "github.com/cyw0ng95/sqlvibe/internal/DS"
	CG "github.com/cyw0ng95/sqlvibe/internal/CG"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// -----------------------------------------------------------------
// Early Termination for LIMIT (v0.9.0 Optimization #5)
// -----------------------------------------------------------------

// BenchmarkLimitEarlyTermination measures SELECT ... LIMIT N without ORDER BY
// on a 10 000-row table.  With early termination the VM halts after collecting
// N rows instead of scanning all rows.
// The result cache is cleared before each iteration for a fair comparison.
func BenchmarkLimitEarlyTermination(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.Run("Limit10_NoOrderBy", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.ClearResultCache()
			b.StartTimer()
			rows := mustQuery(b, db, "SELECT id, val FROM t LIMIT 10")
			for rows.Next() {
			}
		}
	})

	b.Run("Limit100_NoOrderBy", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.ClearResultCache()
			b.StartTimer()
			rows := mustQuery(b, db, "SELECT id, val FROM t LIMIT 100")
			for rows.Next() {
			}
		}
	})

	b.Run("Limit10_WithWhere", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.ClearResultCache()
			b.StartTimer()
			rows := mustQuery(b, db, "SELECT id, val FROM t WHERE val > 100 LIMIT 10")
			for rows.Next() {
			}
		}
	})
}

// BenchmarkSQLite_LimitEarlyTermination is the SQLite baseline for comparison.
func BenchmarkSQLite_LimitEarlyTermination(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 10000; i++ {
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.Run("Limit10_NoOrderBy", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows := mustQuerySQLite(b, db, "SELECT id, val FROM t LIMIT 10")
			for rows.Next() {
			}
			rows.Close()
		}
	})

	b.Run("Limit100_NoOrderBy", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows := mustQuerySQLite(b, db, "SELECT id, val FROM t LIMIT 100")
			for rows.Next() {
			}
			rows.Close()
		}
	})
}

// -----------------------------------------------------------------
// AND Index Lookup / Composite Index Reorder (v0.9.0 Optimization #10)
// -----------------------------------------------------------------

// BenchmarkIndexAndLookup measures a WHERE with two conditions where one
// matches an index.  With the AND index extension, the index is used for
// the first indexable predicate and pre-filters rows before the VM runs.
// The result cache is cleared before each iteration for a fair comparison.
func BenchmarkIndexAndLookup(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE emp (id INTEGER, dept TEXT, salary INTEGER)")
	mustExec(b, db, "CREATE INDEX idx_id ON emp(id)")
	for i := 0; i < 5000; i++ {
		depts := []string{"eng", "mkt", "sales", "hr", "fin"}
		dept := depts[i%5]
		mustExec(b, db, fmt.Sprintf("INSERT INTO emp VALUES (%d, '%s', %d)", i, dept, 30000+i*10))
	}

	// WHERE uses the indexed column (id) AND a non-indexed column (dept).
	// The AND index extension picks up the index on id.
	b.Run("AndIndex_Id_And_Dept", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.ClearResultCache()
			b.StartTimer()
			rows := mustQuery(b, db, "SELECT id, dept, salary FROM emp WHERE id = 42 AND dept = 'eng'")
			for rows.Next() {
			}
		}
	})

	// WHERE with indexed column AND range filter.
	b.Run("AndIndex_Id_And_Salary", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.ClearResultCache()
			b.StartTimer()
			rows := mustQuery(b, db, "SELECT id, salary FROM emp WHERE id = 100 AND salary > 30000")
			for rows.Next() {
			}
		}
	})
}

// BenchmarkSQLite_AndIndex is the SQLite baseline for the AND index lookup.
func BenchmarkSQLite_AndIndex(b *testing.B) {
	db := openSQLiteDB(b)
	defer db.Close()

	mustExecSQLite(b, db, "CREATE TABLE emp (id INTEGER, dept TEXT, salary INTEGER)")
	mustExecSQLite(b, db, "CREATE INDEX idx_id ON emp(id)")
	for i := 0; i < 5000; i++ {
		depts := []string{"eng", "mkt", "sales", "hr", "fin"}
		dept := depts[i%5]
		mustExecSQLite(b, db, fmt.Sprintf("INSERT INTO emp VALUES (%d, '%s', %d)", i, dept, 30000+i*10))
	}

	b.Run("AndIndex_Id_And_Dept", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows := mustQuerySQLite(b, db, "SELECT id, dept, salary FROM emp WHERE id = 42 AND dept = 'eng'")
			for rows.Next() {
			}
			rows.Close()
		}
	})
}

// -----------------------------------------------------------------
// Pre-sized slices (v0.9.0 Optimization #22)
// -----------------------------------------------------------------

// BenchmarkPresizedColumnsWideTable measures SELECT with many columns where
// pre-sizing the column-name result slice reduces reallocations.
// The result cache is cleared before each iteration for a fair comparison.
func BenchmarkPresizedColumnsWideTable(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, `CREATE TABLE wide (
		c1 INTEGER, c2 INTEGER, c3 INTEGER, c4 INTEGER, c5 INTEGER,
		c6 INTEGER, c7 INTEGER, c8 INTEGER, c9 INTEGER, c10 INTEGER
	)`)
	for i := 0; i < 500; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO wide VALUES (%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)",
			i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT c1,c2,c3,c4,c5,c6,c7,c8,c9,c10 FROM wide WHERE c1 > 100")
		for rows.Next() {
		}
	}
}

// -----------------------------------------------------------------
// Statement Pool (v0.9.1)
// -----------------------------------------------------------------

// BenchmarkStatementPool compares repeated Query calls against StatementPool.Get.
// The pool caches compiled prepared statements; repeated Get calls skip re-parsing.
func BenchmarkStatementPool(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE sp_bench (id INTEGER, val TEXT)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO sp_bench VALUES (%d, 'v%d')", i, i))
	}
	const q = "SELECT id, val FROM sp_bench WHERE id > 100"

	b.Run("DirectQuery", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.ClearResultCache()
			b.StartTimer()
			rows := mustQuery(b, db, q)
			for rows.Next() {
			}
		}
	})

	b.Run("StatementPool", func(b *testing.B) {
		pool := sqlvibe.NewStatementPool(db, 50)
		// Warm the pool with one compilation.
		if _, err := pool.Get(q); err != nil {
			b.Fatalf("pool.Get: %v", err)
		}
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			db.ClearResultCache()
			b.StartTimer()
			stmt, err := pool.Get(q)
			if err != nil {
				b.Fatalf("pool.Get: %v", err)
			}
			rows, err := stmt.Query()
			if err != nil {
				b.Fatalf("stmt.Query: %v", err)
			}
			for rows.Next() {
			}
		}
	})
}

// -----------------------------------------------------------------
// Slab Allocator (v0.9.1)
// -----------------------------------------------------------------

// BenchmarkSlabAllocator measures allocation throughput for the slab allocator
// vs plain make([]byte, n) to quantify GC pressure reduction.
func BenchmarkSlabAllocator(b *testing.B) {
	b.Run("SlabAlloc_64", func(b *testing.B) {
		sa := DS.NewSlabAllocator()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = sa.Alloc(64)
			if i%1000 == 0 {
				sa.Reset()
			}
		}
	})

	b.Run("MakeAlloc_64", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = make([]byte, 64)
		}
	})

	b.Run("SlabAlloc_1024", func(b *testing.B) {
		sa := DS.NewSlabAllocator()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = sa.Alloc(1024)
			if i%64 == 0 {
				sa.Reset()
			}
		}
	})

	b.Run("MakeAlloc_1024", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = make([]byte, 1024)
		}
	})
}

// -----------------------------------------------------------------
// Expression Bytecode (v0.9.1)
// -----------------------------------------------------------------

// BenchmarkExprBytecode measures ExprBytecode.Eval throughput for a simple
// arithmetic expression (a + b) vs direct Go addition.
func BenchmarkExprBytecode(b *testing.B) {
	eb := VM.NewExprBytecode()
	ci0 := eb.AddConst(int64(42))
	ci1 := eb.AddConst(int64(58))
	eb.Emit(VM.EOpLoadConst, ci0)
	eb.Emit(VM.EOpLoadConst, ci1)
	eb.Emit(VM.EOpAdd)

	row := []interface{}{int64(1), int64(2)}

	b.Run("ExprBytecode_Add", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = eb.Eval(row)
		}
	})

	b.Run("DirectGoAdd", func(b *testing.B) {
		a, bv := int64(42), int64(58)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = a + bv
		}
	})
}

// -----------------------------------------------------------------
// DirectCompiler IsFastPath (v0.9.1)
// -----------------------------------------------------------------

// BenchmarkDirectCompilerFastPath measures the cost of IsFastPath detection
// for various SQL patterns.
func BenchmarkDirectCompilerFastPath(b *testing.B) {
	queries := []struct {
		name string
		sql  string
	}{
		{"SimpleSelect", "SELECT id, name FROM users WHERE id = 1"},
		{"SelectStar", "SELECT * FROM t"},
		{"WithJoin", "SELECT a.id FROM a JOIN b ON a.id = b.id"},
		{"WithUnion", "SELECT id FROM a UNION SELECT id FROM b"},
	}

	for _, q := range queries {
		q := q
		b.Run(q.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = CG.IsFastPath(q.sql)
			}
		})
	}
}

