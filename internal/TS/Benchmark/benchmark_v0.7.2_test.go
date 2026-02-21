// Package Benchmark provides SQL-level performance benchmarks for sqlvibe.
// This file contains DS Layer benchmarks for B-Tree and storage performance.
package Benchmark

import (
	"fmt"
	"strings"
	"testing"
)

// -----------------------------------------------------------------
// Wave 1: DS Layer - B-Tree & Storage
// Focus: Discover bottlenecks in storage layer
// -----------------------------------------------------------------

// BenchmarkBTreeInsertSequential measures sequential key insert (no split)
func BenchmarkBTreeInsertSequential(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}
}

// BenchmarkBTreeInsertRandom measures random key insert (with split)
func BenchmarkBTreeInsertRandom(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mustExec(b, db, "DELETE FROM t")
		b.StartTimer()
		for j := 0; j < 1000; j++ {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d)", (i*1000+j)*17%10000))
		}
	}
}

// BenchmarkBTreeSearchHit measures B-Tree search for existing key
func BenchmarkBTreeSearchHit(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE id = 500")
	}
}

// BenchmarkBTreeSearchMiss measures B-Tree search for non-existing key
func BenchmarkBTreeSearchMiss(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE id = 9999")
	}
}

// BenchmarkBTreeDelete measures B-Tree delete operation
func BenchmarkBTreeDelete(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExec(b, db, "DELETE FROM t")
		for j := 0; j < 100; j++ {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
		}
	}
}

// BenchmarkPageAllocation measures page allocation overhead
func BenchmarkPageAllocation(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'data%d')", i, i))
	}
}

// BenchmarkVarintEncoding measures varint encoding performance
func BenchmarkVarintEncoding(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE val > 500")
	}
}

// BenchmarkOverflowPage measures overflow page handling
func BenchmarkOverflowPage(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, data BLOB)")

	// Large payload triggers overflow page allocation
	largeData := strings.Repeat("a", 1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, '%s')", i, largeData))
	}
}

// -----------------------------------------------------------------
// Wave 2: VM Layer - Execution Engine
// Focus: Discover bottlenecks in virtual machine
// -----------------------------------------------------------------

// BenchmarkVMInstructionCount measures basic SELECT instruction count
func BenchmarkVMInstructionCount(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT id+1, val*2 FROM t WHERE id < 10")
	}
}

// BenchmarkVMCursorOpen measures cursor open overhead
func BenchmarkVMCursorOpen(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY, val INTEGER)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t")
	}
}

// BenchmarkVMRegisterAlloc measures register allocation/deallocation
func BenchmarkVMRegisterAlloc(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER, d INTEGER, e INTEGER)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d, %d, %d)", i, i+1, i+2, i+3, i+4))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT a+1, b+2, c+3, d+4, e*2 FROM t")
	}
}

// BenchmarkVMCopyOnWrite measures copy vs reference overhead
func BenchmarkVMCopyOnWrite(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val TEXT)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, 'value%d')", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT id, val, val, val FROM t")
	}
}

// BenchmarkVMFunctionCall measures function call overhead
func BenchmarkVMFunctionCall(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT ABS(val), LENGTH('test'), UPPER('hello') FROM t")
	}
}

// BenchmarkVMExpressionEval measures complex expression evaluation
func BenchmarkVMExpressionEval(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d)", i, i*2, i*3))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT (a + b) * c, (a - b) / 2, a % b FROM t")
	}
}

// -----------------------------------------------------------------
// Wave 4: TM Layer - Transactions & WAL
// Focus: Discover bottlenecks in transaction management
// -----------------------------------------------------------------

// BenchmarkTMTransactionBegin measures transaction begin
func BenchmarkTMTransactionBegin(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin()
		tx.Rollback()
	}
}

// BenchmarkTMTransactionCommit measures transaction commit
func BenchmarkTMTransactionCommit(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin()
		tx.Exec(fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
		tx.Commit()
	}
}

// BenchmarkTMTransactionRollback measures transaction rollback
func BenchmarkTMTransactionRollback(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin()
		tx.Exec(fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
		tx.Rollback()
	}
}

// BenchmarkTMLockContention measures lock acquire/release
func BenchmarkTMLockContention(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER PRIMARY KEY)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d)", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin()
		mustQuery(b, db, "SELECT * FROM t")
		tx.Commit()
	}
}

// BenchmarkWALWriteFrame measures WAL frame write
func BenchmarkWALWriteFrame(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "PRAGMA journal_mode = WAL")
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val TEXT)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tx, _ := db.Begin()
		tx.Exec(fmt.Sprintf("INSERT INTO t VALUES (%d, 'value')", i))
		tx.Commit()
	}
}

// -----------------------------------------------------------------
// Wave 5: Edge Cases & Data Patterns
// Focus: Discover performance issues with various data patterns
// -----------------------------------------------------------------

// BenchmarkEdgeEmptyTable measures SELECT on empty table
func BenchmarkEdgeEmptyTable(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t")
	}
}

// BenchmarkEdgeSingleRow measures SELECT on single row
func BenchmarkEdgeSingleRow(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER)")
	mustExec(b, db, "INSERT INTO t VALUES (1)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t")
	}
}

// BenchmarkEdgeAllNulls measures table with all NULL values
func BenchmarkEdgeAllNulls(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, NULL)", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE val IS NULL")
	}
}

// BenchmarkEdgeDuplicateKeys measures many duplicate index keys
func BenchmarkEdgeDuplicateKeys(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, category INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i%10))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE category = 5")
	}
}

// BenchmarkEdgeWideRow measures very wide rows
func BenchmarkEdgeWideRow(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	cols := make([]string, 20)
	for i := 0; i < 20; i++ {
		cols[i] = fmt.Sprintf("col%d INTEGER", i)
	}
	mustExec(b, db, fmt.Sprintf("CREATE TABLE t (%s)", strings.Join(cols, ", ")))

	for i := 0; i < 100; i++ {
		vals := make([]string, 20)
		for j := 0; j < 20; j++ {
			vals[j] = fmt.Sprintf("%d", i+j)
		}
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%s)", strings.Join(vals, ", ")))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t")
	}
}

// BenchmarkEdgeLongVarchar measures long VARCHAR handling
func BenchmarkEdgeLongVarchar(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, data TEXT)")
	longStr := strings.Repeat("a", 1000)

	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, '%s')", i, longStr))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE LENGTH(data) > 500")
	}
}

// BenchmarkEdgeNegativeNumbers measures negative number handling
func BenchmarkEdgeNegativeNumbers(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i-500))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE val < 0")
	}
}

// BenchmarkEdgeDateTime measures DateTime operations
func BenchmarkEdgeDateTime(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, dt TEXT)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, '2024-01-%02d 12:00:00')", i, i%28+1))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE dt > '2024-01-15'")
	}
}

// -----------------------------------------------------------------
// Wave 6: Complex Queries
// Focus: Discover bottlenecks in complex query patterns
// -----------------------------------------------------------------

// BenchmarkQueryNested3Level measures 3-level nested subquery
func BenchmarkQueryNested3Level(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, `
			SELECT * FROM t WHERE id IN (
				SELECT id FROM t WHERE id IN (
					SELECT id FROM t WHERE val > 0
				)
			)
		`)
	}
}

// BenchmarkQuerySelfJoin measures self-join performance
func BenchmarkQuerySelfJoin(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (id INTEGER, parent_id INTEGER, val INTEGER)")
	for i := 1; i <= 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d)", i, i/2, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT a.*, b.* FROM t a JOIN t b ON a.parent_id = b.id")
	}
}

// BenchmarkQueryCrossJoin measures cross join (cartesian)
func BenchmarkQueryCrossJoin(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE a (id INTEGER)")
	mustExec(b, db, "CREATE TABLE b (id INTEGER)")
	for i := 0; i < 20; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO a VALUES (%d)", i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO b VALUES (%d)", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM a, b")
	}
}

// BenchmarkQueryCompositeWhere measures multiple AND/OR conditions
func BenchmarkQueryCompositeWhere(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (a INTEGER, b INTEGER, c INTEGER, d INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d, %d, %d)", i, i%10, i%5, i%2))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE a > 10 AND b < 8 AND c = 3 OR d = 1")
	}
}

// BenchmarkQueryMultipleJoins measures multiple table joins
func BenchmarkQueryMultipleJoins(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t1 (id INTEGER, v1 INTEGER)")
	mustExec(b, db, "CREATE TABLE t2 (id INTEGER, v2 INTEGER)")
	mustExec(b, db, "CREATE TABLE t3 (id INTEGER, v3 INTEGER)")
	for i := 0; i < 50; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t1 VALUES (%d, %d)", i, i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO t2 VALUES (%d, %d)", i, i))
		mustExec(b, db, fmt.Sprintf("INSERT INTO t3 VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t2.id = t3.id")
	}
}

// BenchmarkQueryGroupByMultiple measures multiple GROUP BY
func BenchmarkQueryGroupByMultiple(b *testing.B) {
	db := openDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE t (region TEXT, category TEXT, amount INTEGER)")
	regions := []string{"North", "South", "East", "West"}
	categories := []string{"A", "B", "C"}
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES ('%s', '%s', %d)",
			regions[i%4], categories[i%3], i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT region, category, SUM(amount) FROM t GROUP BY region, category")
	}
}
