// Package Benchmark provides v0.9.1 performance benchmarks.
// These benchmarks cover the v0.9.1 additional optimizations:
//   - Early termination for LIMIT (stops VM after N rows are collected)
//   - AND index lookup (uses index on one sub-predicate of a compound AND)
//   - Pre-sized result slices (reduces allocations in column-name building)
package Benchmark

import (
	"fmt"
	"testing"
)

// -----------------------------------------------------------------
// Early Termination for LIMIT (v0.9.1 Optimization #5)
// -----------------------------------------------------------------

// BenchmarkLimitEarlyTermination measures SELECT ... LIMIT N without ORDER BY
// on a 10 000-row table.  With early termination the VM halts after collecting
// N rows instead of scanning all rows.
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
			rows := mustQuery(b, db, "SELECT id, val FROM t LIMIT 10")
			for rows.Next() {
			}
		}
	})

	b.Run("Limit100_NoOrderBy", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			rows := mustQuery(b, db, "SELECT id, val FROM t LIMIT 100")
			for rows.Next() {
			}
		}
	})

	b.Run("Limit10_WithWhere", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
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
// AND Index Lookup / Composite Index Reorder (v0.9.1 Optimization #10)
// -----------------------------------------------------------------

// BenchmarkIndexAndLookup measures a WHERE with two conditions where one
// matches an index.  With the AND index extension, the index is used for
// the first indexable predicate and pre-filters rows before the VM runs.
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
			rows := mustQuery(b, db, "SELECT id, dept, salary FROM emp WHERE id = 42 AND dept = 'eng'")
			for rows.Next() {
			}
		}
	})

	// WHERE with two indexed column conditions.
	b.Run("AndIndex_Id_And_Salary", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
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
// Pre-sized slices (v0.9.1 Optimization #22)
// -----------------------------------------------------------------

// BenchmarkPresizedColumnsWideTable measures SELECT with many columns where
// pre-sizing the column-name result slice reduces reallocations.
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
		rows := mustQuery(b, db, "SELECT c1,c2,c3,c4,c5,c6,c7,c8,c9,c10 FROM wide WHERE c1 > 100")
		for rows.Next() {
		}
	}
}
