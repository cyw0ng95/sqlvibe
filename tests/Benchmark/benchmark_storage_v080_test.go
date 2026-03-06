// Package Benchmark provides SQL-level performance benchmarks for sqlvibe.
// This file contains v0.8.0 storage-layer benchmarks comparing the C++ columnar
// HybridStore (accessed via SQL) against SQLite for equivalent operations.
// In v0.11.2+ the HybridStore is implemented in C++ (src/core/DS/);
// all benchmarks use the public SQL API which routes through the C++ layer.
package Benchmark

import (
	"fmt"
	"testing"
)

// -----------------------------------------------------------------
// BenchmarkStorage_Insert_1K – insert 1 000 rows
// -----------------------------------------------------------------

func BenchmarkStorage_Insert_1K_Sqlvibe(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := openDB(b)
		mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
		b.StartTimer()
		for j := 0; j < 1000; j++ {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
		}
		b.StopTimer()
		db.Close()
	}
}

func BenchmarkStorage_Insert_1K_SQLite(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := openSQ(b)
		sqExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
		b.StartTimer()
		for j := 0; j < 1000; j++ {
			sqExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
		}
		b.StopTimer()
		db.Close()
	}
}

// -----------------------------------------------------------------
// BenchmarkStorage_ScanAll_1K – full table scan of 1 000 rows
// -----------------------------------------------------------------

func BenchmarkStorage_ScanAll_1K_Sqlvibe(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT * FROM t")
		for rows.Next() {
		}
	}
}

func BenchmarkStorage_ScanAll_1K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	sqExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		sqExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT * FROM t")
	}
}

// -----------------------------------------------------------------
// BenchmarkStorage_FilterEqual_1K – equality filter on 1 000 rows
// -----------------------------------------------------------------

func BenchmarkStorage_FilterEqual_1K_Sqlvibe(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j%10))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT * FROM t WHERE val = 5")
		for rows.Next() {
		}
	}
}

func BenchmarkStorage_FilterEqual_1K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	sqExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		sqExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j%10))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT * FROM t WHERE val = 5")
	}
}

// -----------------------------------------------------------------
// BenchmarkStorage_ColumnarSum_1K – SUM of 1 000 int values
// -----------------------------------------------------------------

func BenchmarkStorage_ColumnarSum_1K_Sqlvibe(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
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
}

func BenchmarkStorage_ColumnarSum_1K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	sqExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		sqExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT SUM(val) FROM t")
	}
}

// -----------------------------------------------------------------
// BenchmarkStorage_ColumnarCount_1K – COUNT of 1 000 rows
// -----------------------------------------------------------------

func BenchmarkStorage_ColumnarCount_1K_Sqlvibe(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT COUNT(*) FROM t")
		for rows.Next() {
		}
	}
}

func BenchmarkStorage_ColumnarCount_1K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	sqExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		sqExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

// -----------------------------------------------------------------
// BenchmarkStorage_GroupBy_1K – GROUP BY on 1 000 rows
// -----------------------------------------------------------------

func BenchmarkStorage_GroupBy_1K_Sqlvibe(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (cat TEXT, val INTEGER)")
	cats := []string{"A", "B", "C", "D"}
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES ('%s', %d)", cats[j%len(cats)], j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT cat, SUM(val) FROM t GROUP BY cat")
		for rows.Next() {
		}
	}
}

func BenchmarkStorage_GroupBy_1K_SQLite(b *testing.B) {
	db := openSQ(b)
	defer db.Close()
	sqExec(b, db, "CREATE TABLE t (cat TEXT, val INTEGER)")
	cats := []string{"A", "B", "C", "D"}
	for j := 0; j < 1000; j++ {
		sqExec(b, db, fmt.Sprintf("INSERT INTO t VALUES ('%s', %d)", cats[j%len(cats)], j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sqQuery(b, db, "SELECT cat, SUM(val) FROM t GROUP BY cat")
	}
}

// BenchmarkStorage_MemoryProfile_* — allocation profiles
// -----------------------------------------------------------------

// BenchmarkStorage_MemoryProfile_Insert measures per-INSERT allocation via SQL.
func BenchmarkStorage_MemoryProfile_Insert(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db := openDB(b)
		mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
		b.StartTimer()
		for j := 0; j < 100; j++ {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
		}
		b.StopTimer()
		db.Close()
	}
}

// BenchmarkStorage_MemoryProfile_Filter measures per-scan allocation via SQL.
func BenchmarkStorage_MemoryProfile_Filter(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j%100))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT * FROM t WHERE val = 42")
		for rows.Next() {
		}
	}
}

// BenchmarkStorage_MemoryProfile_ColumnarSum measures per-SUM allocation via SQL.
func BenchmarkStorage_MemoryProfile_ColumnarSum(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT SUM(val) FROM t")
		for rows.Next() {
		}
	}
}

// BenchmarkStorage_MemoryProfile_GroupBy measures GROUP BY allocation via SQL.
func BenchmarkStorage_MemoryProfile_GroupBy(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (cat TEXT, val INTEGER)")
	cats := []string{"A", "B", "C", "D"}
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES ('%s', %d)", cats[j%len(cats)], j))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows := mustQuery(b, db, "SELECT cat, SUM(val) FROM t GROUP BY cat")
		for rows.Next() {
		}
	}
}

