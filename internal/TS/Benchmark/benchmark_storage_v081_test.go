// Package Benchmark provides SQL-level performance benchmarks for sqlvibe.
// This file contains v0.8.1 storage-layer benchmarks for the new columnar
// opcodes, parallel aggregation, and parallel scan features.
package Benchmark

import (
	"testing"

	"github.com/sqlvibe/sqlvibe/internal/DS"
)

// -----------------------------------------------------------------
// helpers
// -----------------------------------------------------------------

func makeStore1K() *DS.HybridStore {
	hs := DS.NewHybridStore(
		[]string{"id", "val"},
		[]DS.ValueType{DS.TypeInt, DS.TypeInt},
	)
	for j := 0; j < 1000; j++ {
		hs.Insert(intRow(j, j))
	}
	return hs
}

func makeStore100K() *DS.HybridStore {
	hs := DS.NewHybridStore(
		[]string{"id", "val"},
		[]DS.ValueType{DS.TypeInt, DS.TypeInt},
	)
	for j := 0; j < 100000; j++ {
		hs.Insert(intRow(j, j))
	}
	return hs
}

// -----------------------------------------------------------------
// Phase 1: Columnar opcode benchmarks (via storage layer)
// -----------------------------------------------------------------

// BenchmarkColumnarScan_1K benchmarks a full columnar scan of 1K rows.
func BenchmarkColumnarScan_1K(b *testing.B) {
	hs := makeStore1K()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hs.Scan()
	}
}

// BenchmarkColumnarFilter_1K benchmarks a range filter scan of 1K rows.
func BenchmarkColumnarFilter_1K(b *testing.B) {
	hs := makeStore1K()
	filterVal := DS.IntValue(500)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hs.ScanWithFilter("val", ">=", filterVal)
	}
}

// BenchmarkColumnarCount_1K benchmarks COUNT(*) via LiveCount on 1K rows.
func BenchmarkColumnarCount_1K(b *testing.B) {
	hs := makeStore1K()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hs.LiveCount()
	}
}

// BenchmarkColumnarSum_1K benchmarks ParallelSum on 1K rows.
func BenchmarkColumnarSum_1K(b *testing.B) {
	hs := makeStore1K()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hs.ParallelSum("val")
	}
}

// -----------------------------------------------------------------
// Phase 7: Parallel scan benchmarks (100K rows)
// -----------------------------------------------------------------

// BenchmarkParallelCount_100K benchmarks ParallelCount on 100K rows.
func BenchmarkParallelCount_100K(b *testing.B) {
	hs := makeStore100K()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hs.ParallelCount()
	}
}

// BenchmarkParallelSum_100K benchmarks ParallelSum on 100K rows.
func BenchmarkParallelSum_100K(b *testing.B) {
	hs := makeStore100K()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hs.ParallelSum("val")
	}
}

// BenchmarkParallelScan_100K benchmarks ParallelScan on 100K rows.
func BenchmarkParallelScan_100K(b *testing.B) {
	hs := makeStore100K()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hs.ParallelScan()
	}
}
