package VM

import (
	"testing"
)

// TestCompareInt64Batch tests batch integer comparison.
func TestCompareInt64Batch(t *testing.T) {
	a := []int64{1, 5, 3, 7}
	b := []int64{2, 5, 1, 4}
	results := make([]int32, 4)
	CompareInt64Batch(a, b, results)
	// 1<2 → -1, 5==5 → 0, 3>1 → 1, 7>4 → 1
	want := []int32{-1, 0, 1, 1}
	for i, w := range want {
		if results[i] != w {
			t.Errorf("results[%d] = %d; want %d", i, results[i], w)
		}
	}
}

// TestCompareFloat64Batch tests batch float comparison.
func TestCompareFloat64Batch(t *testing.T) {
	a := []float64{1.0, 5.0, 3.0}
	b := []float64{2.0, 5.0, 1.0}
	results := make([]int32, 3)
	CompareFloat64Batch(a, b, results)
	want := []int32{-1, 0, 1}
	for i, w := range want {
		if results[i] != w {
			t.Errorf("results[%d] = %d; want %d", i, results[i], w)
		}
	}
}

// TestAddInt64Batch tests batch integer addition.
func TestAddInt64Batch(t *testing.T) {
	a := []int64{1, 2, 3}
	b := []int64{4, 5, 6}
	results := make([]int64, 3)
	AddInt64Batch(a, b, results)
	want := []int64{5, 7, 9}
	for i, w := range want {
		if results[i] != w {
			t.Errorf("results[%d] = %d; want %d", i, results[i], w)
		}
	}
}

// TestSubInt64Batch tests batch integer subtraction.
func TestSubInt64Batch(t *testing.T) {
	a := []int64{10, 20, 30}
	b := []int64{1, 2, 3}
	results := make([]int64, 3)
	SubInt64Batch(a, b, results)
	want := []int64{9, 18, 27}
	for i, w := range want {
		if results[i] != w {
			t.Errorf("results[%d] = %d; want %d", i, results[i], w)
		}
	}
}

// TestMulInt64Batch tests batch integer multiplication.
func TestMulInt64Batch(t *testing.T) {
	a := []int64{2, 3, 4}
	b := []int64{5, 6, 7}
	results := make([]int64, 3)
	MulInt64Batch(a, b, results)
	want := []int64{10, 18, 28}
	for i, w := range want {
		if results[i] != w {
			t.Errorf("results[%d] = %d; want %d", i, results[i], w)
		}
	}
}

// TestAddFloat64Batch tests batch float addition.
func TestAddFloat64Batch(t *testing.T) {
	a := []float64{1.5, 2.5, 3.5}
	b := []float64{0.5, 0.5, 0.5}
	results := make([]float64, 3)
	AddFloat64Batch(a, b, results)
	want := []float64{2.0, 3.0, 4.0}
	for i, w := range want {
		if results[i] != w {
			t.Errorf("results[%d] = %f; want %f", i, results[i], w)
		}
	}
}

// TestFilterMask tests the boolean mask filter.
func TestFilterMask(t *testing.T) {
	mask := []int8{1, 0, 1, 0, 1}
	indices := []int64{10, 20, 30, 40, 50}
	out := FilterMask(mask, indices)
	want := []int64{10, 30, 50}
	if len(out) != len(want) {
		t.Fatalf("FilterMask length = %d; want %d", len(out), len(want))
	}
	for i, w := range want {
		if out[i] != w {
			t.Errorf("out[%d] = %d; want %d", i, out[i], w)
		}
	}
}

// TestFilterMask_Empty tests FilterMask with empty inputs.
func TestFilterMask_Empty(t *testing.T) {
	out := FilterMask(nil, nil)
	if out != nil {
		t.Error("expected nil for empty inputs")
	}
}

// TestBatchOps_Empty tests that batch ops handle empty slices gracefully.
func TestBatchOps_Empty(t *testing.T) {
	CompareInt64Batch(nil, nil, nil)
	CompareFloat64Batch(nil, nil, nil)
	AddInt64Batch(nil, nil, nil)
	SubInt64Batch(nil, nil, nil)
	MulInt64Batch(nil, nil, nil)
	AddFloat64Batch(nil, nil, nil)
	SubFloat64Batch(nil, nil, nil)
	MulFloat64Batch(nil, nil, nil)
}
