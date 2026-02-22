// Package Benchmark provides SQL-level performance benchmarks for sqlvibe.
// This file contains v0.8.0 storage-layer benchmarks comparing the new columnar
// HybridStore against the existing row-based sqlvibe.DB SQL interface.
package Benchmark

import (
	"fmt"
	"testing"

	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe"
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe/storage"
)

// -----------------------------------------------------------------
// helpers
// -----------------------------------------------------------------

func newHybridStore() *storage.HybridStore {
	return storage.NewHybridStore(
		[]string{"id", "val"},
		[]storage.ValueType{storage.TypeInt, storage.TypeInt},
	)
}

func intRow(id, val int) []storage.Value {
	return []storage.Value{storage.IntValue(int64(id)), storage.IntValue(int64(val))}
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
	target := storage.IntValue(5)
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
	target := storage.IntValue(5)
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
	rb1 := storage.NewRoaringBitmap()
	rb2 := storage.NewRoaringBitmap()
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
