// Package Benchmark provides v0.10.0 performance benchmarks for the bytecode engine.
package Benchmark

import (
	"fmt"
	"testing"

	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe"
)

func openBCDB(b *testing.B) *sqlvibe.Database {
	b.Helper()
	db, err := sqlvibe.Open(":memory:")
	if err != nil {
		b.Fatal(err)
	}
	return db
}

// BenchmarkBC_SelectAll1K benchmarks SELECT all rows from a 1000-row integer table.
// Target: −25% time/op vs v0.9.x (no interface{} boxing).
func BenchmarkBC_SelectAll1K(b *testing.B) {
	db := openBCDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE bench1k(id INTEGER, val INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO bench1k VALUES (%d, %d)", i, i*2))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows, err := db.Query("SELECT id, val FROM bench1k")
		if err != nil {
			b.Fatal(err)
		}
		_ = rows
	}
}

// BenchmarkBC_ArithInt benchmarks direct integer arithmetic in the bytecode VM.
// Target: −40% time/op (direct int64 add, no dispatch overhead).
func BenchmarkBC_ArithInt(b *testing.B) {
	db := openBCDB(b)
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := db.Query("SELECT 1 + 2 + 3 + 4 + 5")
		if err != nil {
			b.Fatal(err)
		}
		_ = rows
	}
}

// BenchmarkBC_WhereFilter benchmarks WHERE filtering on a 1000-row integer table.
// Target: −30% time/op (typed int fast path).
func BenchmarkBC_WhereFilter(b *testing.B) {
	db := openBCDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE bwhere(n INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO bwhere VALUES (%d)", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows, err := db.Query("SELECT n FROM bwhere WHERE n > 500")
		if err != nil {
			b.Fatal(err)
		}
		_ = rows
	}
}

// BenchmarkBC_SumAggregate benchmarks SUM aggregate via the legacy path (bytecode does not
// yet support aggregates from table scans, so this exercises the fallback).
// Target: −20% vs v0.9.x.
func BenchmarkBC_SumAggregate(b *testing.B) {
	db := openBCDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE bsum(n INTEGER)")
	for i := 0; i < 1000; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO bsum VALUES (%d)", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows, err := db.Query("SELECT SUM(n) FROM bsum")
		if err != nil {
			b.Fatal(err)
		}
		_ = rows
	}
}

// BenchmarkBC_Allocs benchmarks allocation count for an all-integer SELECT.
func BenchmarkBC_Allocs(b *testing.B) {
	db := openBCDB(b)
	defer db.Close()

	mustExec(b, db, "CREATE TABLE balloc(id INTEGER, v INTEGER)")
	for i := 0; i < 100; i++ {
		mustExec(b, db, fmt.Sprintf("INSERT INTO balloc VALUES (%d, %d)", i, i))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		db.ClearResultCache()
		b.StartTimer()
		rows, err := db.Query("SELECT id, v FROM balloc")
		if err != nil {
			b.Fatal(err)
		}
		_ = rows
	}
}
