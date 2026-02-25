// Package Benchmark provides SQL-level performance benchmarks for sqlvibe.
// This file contains v0.8.0 storage-layer benchmarks comparing the new columnar
// HybridStore against the existing row-based sqlvibe.DB SQL interface.
package Benchmark

import (
	"fmt"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

// -----------------------------------------------------------------
// helpers
// -----------------------------------------------------------------

func newHybridStore() *DS.HybridStore {
	return DS.NewHybridStore(
		[]string{"id", "val"},
		[]DS.ValueType{DS.TypeInt, DS.TypeInt},
	)
}

func intRow(id, val int) []DS.Value {
	return []DS.Value{DS.IntValue(int64(id)), DS.IntValue(int64(val))}
}

// -----------------------------------------------------------------
// BenchmarkStorage_Insert_1K – insert 1 000 rows
// -----------------------------------------------------------------

func BenchmarkStorage_Insert_1K_HybridStore(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hs := newHybridStore()
		for j := 0; j < 1000; j++ {
			hs.Insert(intRow(j, j))
		}
	}
}

func BenchmarkStorage_Insert_1K_SqlvibeSQLDB(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		mustExec(b, db, "DELETE FROM t")
		b.StartTimer()
		for j := 0; j < 1000; j++ {
			mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
		}
	}
}

// -----------------------------------------------------------------
// BenchmarkStorage_ScanAll_1K – full table scan of 1 000 rows
// -----------------------------------------------------------------

func BenchmarkStorage_ScanAll_1K_HybridStore(b *testing.B) {
	hs := newHybridStore()
	for j := 0; j < 1000; j++ {
		hs.Insert(intRow(j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hs.Scan()
	}
}

func BenchmarkStorage_ScanAll_1K_SqlvibeSQLDB(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t")
	}
}

// -----------------------------------------------------------------
// BenchmarkStorage_FilterEqual_1K – equality filter on 1 000 rows
// -----------------------------------------------------------------

func BenchmarkStorage_FilterEqual_1K_HybridStore(b *testing.B) {
	hs := newHybridStore()
	for j := 0; j < 1000; j++ {
		hs.Insert(intRow(j, j%10))
	}
	target := DS.IntValue(5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hs.ScanWhere("val", target)
	}
}

func BenchmarkStorage_FilterEqual_1K_VectorizedFilter(b *testing.B) {
	hs := newHybridStore()
	for j := 0; j < 1000; j++ {
		hs.Insert(intRow(j, j%10))
	}
	col := hs.ColStore().GetColumn("val")
	target := DS.IntValue(5)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sqlvibe.VectorizedFilter(col, "=", target)
	}
}

func BenchmarkStorage_FilterEqual_1K_SqlvibeSQLDB(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j%10))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT * FROM t WHERE val = 5")
	}
}

// -----------------------------------------------------------------
// BenchmarkStorage_ColumnarSum_1K – SUM of 1 000 int values
// -----------------------------------------------------------------

func BenchmarkStorage_ColumnarSum_1K_HybridStore(b *testing.B) {
	hs := newHybridStore()
	for j := 0; j < 1000; j++ {
		hs.Insert(intRow(j, j))
	}
	col := hs.ColStore().GetColumn("val")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sqlvibe.ColumnarSum(col)
	}
}

func BenchmarkStorage_ColumnarSum_1K_SqlvibeSQLDB(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT SUM(val) FROM t")
	}
}

// -----------------------------------------------------------------
// BenchmarkStorage_ColumnarCount_1K – COUNT of 1 000 rows
// -----------------------------------------------------------------

func BenchmarkStorage_ColumnarCount_1K_HybridStore(b *testing.B) {
	hs := newHybridStore()
	for j := 0; j < 1000; j++ {
		hs.Insert(intRow(j, j))
	}
	col := hs.ColStore().GetColumn("val")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sqlvibe.ColumnarCount(col)
	}
}

func BenchmarkStorage_ColumnarCount_1K_SqlvibeSQLDB(b *testing.B) {
	db := openDB(b)
	defer db.Close()
	mustExec(b, db, "CREATE TABLE t (id INTEGER, val INTEGER)")
	for j := 0; j < 1000; j++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO t VALUES (%d, %d)", j, j))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mustQuery(b, db, "SELECT COUNT(*) FROM t")
	}
}

// -----------------------------------------------------------------
// BenchmarkStorage_RoaringBitmap_AndFilter – AND on two 10K bitmaps
// -----------------------------------------------------------------

func BenchmarkStorage_RoaringBitmap_AndFilter(b *testing.B) {
	rb1 := DS.NewRoaringBitmap()
	rb2 := DS.NewRoaringBitmap()
	for i := uint32(0); i < 10000; i++ {
		if i%2 == 0 {
			rb1.Add(i)
		}
		if i%3 == 0 {
			rb2.Add(i)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = rb1.And(rb2)
	}
}

// -----------------------------------------------------------------
// BenchmarkStorage_MemoryProfile_* – memory allocation profiles
// These benchmarks measure allocations per operation, allowing
// memory profiling via: go test -bench=MemoryProfile -memprofile=mem.prof
// -----------------------------------------------------------------

// BenchmarkStorage_MemoryProfile_HybridInsert measures allocations for batch inserts.
func BenchmarkStorage_MemoryProfile_HybridInsert(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		hs := newHybridStore()
		for j := 0; j < 1000; j++ {
			hs.Insert(intRow(j, j))
		}
	}
}

// BenchmarkStorage_MemoryProfile_VectorFilter measures allocations for vectorized filter.
func BenchmarkStorage_MemoryProfile_VectorFilter(b *testing.B) {
	hs := newHybridStore()
	for j := 0; j < 1000; j++ {
		hs.Insert(intRow(j, j%100))
	}
	col := hs.ColStore().GetColumn("val")
	target := DS.IntValue(42)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sqlvibe.VectorizedFilter(col, "=", target)
	}
}

// BenchmarkStorage_MemoryProfile_ColumnarSum measures allocations for columnar sum.
func BenchmarkStorage_MemoryProfile_ColumnarSum(b *testing.B) {
	hs := newHybridStore()
	for j := 0; j < 1000; j++ {
		hs.Insert(intRow(j, j))
	}
	col := hs.ColStore().GetColumn("val")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sqlvibe.ColumnarSum(col)
	}
}

// BenchmarkStorage_MemoryProfile_ColumnarGroupBy measures allocations for columnar GROUP BY.
func BenchmarkStorage_MemoryProfile_ColumnarGroupBy(b *testing.B) {
	hs := DS.NewHybridStore(
		[]string{"cat", "val"},
		[]DS.ValueType{DS.TypeString, DS.TypeInt},
	)
	cats := []string{"A", "B", "C", "D"}
	for j := 0; j < 1000; j++ {
		hs.Insert([]DS.Value{
			DS.StringValue(cats[j%len(cats)]),
			DS.IntValue(int64(j)),
		})
	}
	keyCol := hs.ColStore().GetColumn("cat")
	valCol := hs.ColStore().GetColumn("val")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = sqlvibe.ColumnarGroupBy(keyCol, valCol, "sum")
	}
}

// BenchmarkStorage_GCProfile_HybridScan measures GC pressure during a full scan.
// Run with: go test -bench=GCProfile -gcflags="-m" to see escape analysis.
func BenchmarkStorage_GCProfile_HybridScan(b *testing.B) {
	hs := newHybridStore()
	for j := 0; j < 5000; j++ {
		hs.Insert(intRow(j, j))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows := hs.Scan()
		_ = rows
	}
}

// BenchmarkStorage_Compression_RLE_Encode benchmarks RLE encoding throughput.
func BenchmarkStorage_Compression_RLE_Encode(b *testing.B) {
	// Simulate a column with low cardinality (good for RLE).
	data := make([]byte, 1024)
	for i := range data {
		data[i] = byte(i / 100) // 11 distinct values, long runs
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = DS.BenchEncodeRLE(data)
	}
}
