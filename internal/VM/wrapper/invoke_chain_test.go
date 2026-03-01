// Package wrapper tests: verify invoke chain CGO functions work correctly.
package wrapper

import (
	"testing"
)

// ─── Phase 4.2: PipelineHashFilter ───────────────────────────────────────────

func TestPipelineHashFilter_Basic(t *testing.T) {
	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("date"),
		[]byte("elderberry"),
	}
	// With bucketCount=2, roughly half the keys should land in bucket 0.
	indices := PipelineHashFilter(keys, 0, 2, 0)
	// Verify all returned indices are within bounds.
	for _, idx := range indices {
		if idx < 0 || idx >= len(keys) {
			t.Errorf("index %d out of range [0, %d)", idx, len(keys))
		}
	}
}

func TestPipelineHashFilter_Empty(t *testing.T) {
	if got := PipelineHashFilter(nil, 0, 4, 0); got != nil {
		t.Errorf("expected nil for empty input, got %v", got)
	}
}

func TestPipelineHashFilter_ZeroBuckets(t *testing.T) {
	keys := [][]byte{[]byte("a")}
	if got := PipelineHashFilter(keys, 0, 0, 0); got != nil {
		t.Errorf("expected nil for zero bucket count, got %v", got)
	}
}

func TestPipelineHashFilter_AllInSameBucket(t *testing.T) {
	// Single key, single bucket → must always match bucket 0.
	keys := [][]byte{[]byte("only")}
	indices := PipelineHashFilter(keys, 0, 1, 0)
	if len(indices) != 1 || indices[0] != 0 {
		t.Errorf("expected [0], got %v", indices)
	}
}

// ─── Phase 4.3: BatchEvalCompareInt64 ────────────────────────────────────────

func TestBatchEvalCompareInt64_EQ(t *testing.T) {
	a := []int64{1, 2, 3, 4, 5}
	b := []int64{1, 0, 3, 0, 5}
	mask, count := BatchEvalCompareInt64(a, b, CmpEQ)
	if count != 3 {
		t.Errorf("expected 3 matches, got %d", count)
	}
	expect := []byte{1, 0, 1, 0, 1}
	for i, v := range expect {
		if mask[i] != v {
			t.Errorf("mask[%d]: expected %d, got %d", i, v, mask[i])
		}
	}
}

func TestBatchEvalCompareInt64_LT(t *testing.T) {
	a := []int64{1, 5, 3, 7}
	b := []int64{5, 5, 5, 5}
	mask, count := BatchEvalCompareInt64(a, b, CmpLT)
	if count != 2 {
		t.Errorf("expected 2 matches, got %d", count)
	}
	// 1<5 ✓, 5<5 ✗, 3<5 ✓, 7<5 ✗
	expect := []byte{1, 0, 1, 0}
	for i, v := range expect {
		if mask[i] != v {
			t.Errorf("mask[%d]: expected %d, got %d", i, v, mask[i])
		}
	}
}

func TestBatchEvalCompareInt64_Empty(t *testing.T) {
	mask, count := BatchEvalCompareInt64(nil, nil, CmpEQ)
	if mask != nil || count != 0 {
		t.Error("expected nil mask and 0 count for empty input")
	}
}

func TestBatchEvalCompareFloat64_GE(t *testing.T) {
	a := []float64{1.0, 2.5, 3.0, 0.5}
	b := []float64{1.0, 2.5, 2.9, 1.0}
	mask, count := BatchEvalCompareFloat64(a, b, CmpGE)
	if count != 3 {
		t.Errorf("expected 3 matches, got %d", count)
	}
	// 1.0>=1.0 ✓, 2.5>=2.5 ✓, 3.0>=2.9 ✓, 0.5>=1.0 ✗
	expect := []byte{1, 1, 1, 0}
	for i, v := range expect {
		if mask[i] != v {
			t.Errorf("mask[%d]: expected %d, got %d", i, v, mask[i])
		}
	}
}

func TestBatchArithAndCompareInt64_AddGT(t *testing.T) {
	// (a+b) > 10
	a := []int64{5, 3, 8, 1}
	b := []int64{6, 4, 3, 2}
	mask, count := BatchArithAndCompareInt64(a, b, ArithAdd, 10, CmpGT)
	// 5+6=11>10 ✓, 3+4=7 ✗, 8+3=11>10 ✓, 1+2=3 ✗
	if count != 2 {
		t.Errorf("expected 2 matches, got %d", count)
	}
	expect := []byte{1, 0, 1, 0}
	for i, v := range expect {
		if mask[i] != v {
			t.Errorf("mask[%d]: expected %d, got %d", i, v, mask[i])
		}
	}
}

func TestBatchArithAndCompareInt64_SubLE(t *testing.T) {
	// (a-b) <= 0
	a := []int64{3, 5, 2}
	b := []int64{3, 3, 4}
	_, count := BatchArithAndCompareInt64(a, b, ArithSub, 0, CmpLE)
	// 0<=0 ✓, 2 ✗, -2<=0 ✓
	if count != 2 {
		t.Errorf("expected 2 matches, got %d", count)
	}
}

// ─── Phase 4.4: ScanFilterInt64 ──────────────────────────────────────────────

func TestScanFilterInt64_GT(t *testing.T) {
	col := []int64{1, 5, 3, 9, 2, 7}
	idx := ScanFilterInt64(col, CmpGT, 4)
	// 5>4, 9>4, 7>4
	if len(idx) != 3 {
		t.Errorf("expected 3 indices, got %d: %v", len(idx), idx)
	}
}

func TestScanFilterInt64_EQ(t *testing.T) {
	col := []int64{10, 20, 10, 30, 10}
	idx := ScanFilterInt64(col, CmpEQ, 10)
	if len(idx) != 3 {
		t.Errorf("expected 3 indices, got %d", len(idx))
	}
}

func TestScanFilterInt64_Empty(t *testing.T) {
	idx := ScanFilterInt64(nil, CmpEQ, 0)
	if len(idx) != 0 {
		t.Errorf("expected empty result, got %v", idx)
	}
}

func TestScanFilterFloat64_LT(t *testing.T) {
	col := []float64{1.5, 3.0, 0.5, 2.0}
	idx := ScanFilterFloat64(col, CmpLT, 2.0)
	// 1.5<2.0, 0.5<2.0
	if len(idx) != 2 {
		t.Errorf("expected 2 indices, got %d: %v", len(idx), idx)
	}
}

// ─── ScanAggregateInt64 ───────────────────────────────────────────────────────

func TestScanAggregateInt64_SumNoFilter(t *testing.T) {
	col := []int64{1, 2, 3, 4, 5}
	sum, count := ScanAggregateInt64(col, CmpEQ, 0, false, AggSum)
	if sum != 15 || count != 5 {
		t.Errorf("expected sum=15 count=5, got sum=%d count=%d", sum, count)
	}
}

func TestScanAggregateInt64_SumWithFilter(t *testing.T) {
	col := []int64{1, 2, 3, 4, 5}
	sum, count := ScanAggregateInt64(col, CmpGT, 2, true, AggSum)
	// 3+4+5=12
	if sum != 12 || count != 3 {
		t.Errorf("expected sum=12 count=3, got sum=%d count=%d", sum, count)
	}
}

func TestScanAggregateInt64_Min(t *testing.T) {
	col := []int64{5, 3, 8, 1, 9}
	mn, _ := ScanAggregateInt64(col, CmpEQ, 0, false, AggMin)
	if mn != 1 {
		t.Errorf("expected min=1, got %d", mn)
	}
}

func TestScanAggregateInt64_Max(t *testing.T) {
	col := []int64{5, 3, 8, 1, 9}
	mx, _ := ScanAggregateInt64(col, CmpEQ, 0, false, AggMax)
	if mx != 9 {
		t.Errorf("expected max=9, got %d", mx)
	}
}

func TestScanAggregateInt64_Count(t *testing.T) {
	col := []int64{1, 2, 3, 4, 5}
	cnt, _ := ScanAggregateInt64(col, CmpGT, 3, true, AggCount)
	if cnt != 2 {
		t.Errorf("expected count=2, got %d", cnt)
	}
}

func TestScanAggregateInt64_EmptyColumn(t *testing.T) {
	val, count := ScanAggregateInt64(nil, CmpEQ, 0, false, AggSum)
	if val != 0 || count != 0 {
		t.Errorf("expected 0,0 for empty column, got %d,%d", val, count)
	}
}
