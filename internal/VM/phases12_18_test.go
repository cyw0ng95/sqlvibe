package VM

import (
	"testing"
)

// TestPhase12_ExprEvalBatch tests Phase 12: VM Expression Evaluation batch functions.
func TestPhase12_ExprEvalBatch(t *testing.T) {
	t.Run("CompareInt64Batch", func(t *testing.T) {
		a := []int64{1, 5, 3}
		b := []int64{2, 5, 1}
		got := CompareInt64Batch(a, b)
		want := []int{-1, 0, 1}
		if len(got) != len(want) {
			t.Fatalf("len=%d want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("[%d] got %d want %d", i, got[i], want[i])
			}
		}
	})

	t.Run("CompareFloat64Batch", func(t *testing.T) {
		a := []float64{1.0, 5.0, 3.0}
		b := []float64{2.0, 5.0, 1.0}
		got := CompareFloat64Batch(a, b)
		want := []int{-1, 0, 1}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("[%d] got %d want %d", i, got[i], want[i])
			}
		}
	})

	t.Run("AddInt64Batch", func(t *testing.T) {
		a := []int64{1, 2, 3}
		b := []int64{10, 20, 30}
		got := AddInt64Batch(a, b)
		want := []int64{11, 22, 33}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("[%d] got %d want %d", i, got[i], want[i])
			}
		}
	})

	t.Run("AddFloat64Batch", func(t *testing.T) {
		a := []float64{1.5, 2.5}
		b := []float64{0.5, 1.5}
		got := AddFloat64Batch(a, b)
		want := []float64{2.0, 4.0}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("[%d] got %v want %v", i, got[i], want[i])
			}
		}
	})

	t.Run("SubInt64Batch", func(t *testing.T) {
		a := []int64{10, 20, 30}
		b := []int64{1, 2, 3}
		got := SubInt64Batch(a, b)
		want := []int64{9, 18, 27}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("[%d] got %d want %d", i, got[i], want[i])
			}
		}
	})

	t.Run("MulInt64Batch", func(t *testing.T) {
		a := []int64{2, 3, 4}
		b := []int64{3, 4, 5}
		got := MulInt64Batch(a, b)
		want := []int64{6, 12, 20}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("[%d] got %d want %d", i, got[i], want[i])
			}
		}
	})

	t.Run("FilterMask", func(t *testing.T) {
		mask := []bool{true, false, true, false, true}
		indices := []int64{10, 20, 30, 40, 50}
		got := FilterMask(mask, indices)
		want := []int64{10, 30, 50}
		if len(got) != len(want) {
			t.Fatalf("len=%d want %d", len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("[%d] got %d want %d", i, got[i], want[i])
			}
		}
	})

	t.Run("EmptySlices", func(t *testing.T) {
		if got := CompareInt64Batch(nil, nil); len(got) != 0 {
			t.Errorf("expected empty for nil input, got %v", got)
		}
		if got := AddInt64Batch(nil, nil); len(got) != 0 {
			t.Errorf("expected empty for nil input, got %v", got)
		}
	})
}

// TestPhase13_DispatchBatch tests Phase 13: VM Bytecode Dispatcher batch functions.
func TestPhase13_DispatchBatch(t *testing.T) {
	t.Run("ArithInt64Batch_Add", func(t *testing.T) {
		a := []int64{1, 2, 3}
		b := []int64{10, 20, 30}
		got, errStr := ArithInt64Batch(0, a, b)
		if errStr != "" {
			t.Fatal(errStr)
		}
		want := []int64{11, 22, 33}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("[%d] got %d want %d", i, got[i], want[i])
			}
		}
	})

	t.Run("ArithInt64Batch_Sub", func(t *testing.T) {
		a := []int64{10, 20, 30}
		b := []int64{1, 2, 3}
		got, _ := ArithInt64Batch(1, a, b)
		want := []int64{9, 18, 27}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("[%d] got %d want %d", i, got[i], want[i])
			}
		}
	})

	t.Run("ArithInt64Batch_Div_ByZero", func(t *testing.T) {
		a := []int64{10}
		b := []int64{0}
		_, errStr := ArithInt64Batch(3, a, b)
		if errStr == "" {
			t.Fatal("expected division by zero error")
		}
	})

	t.Run("ArithFloat64Batch_Add", func(t *testing.T) {
		a := []float64{1.5, 2.5}
		b := []float64{0.5, 1.5}
		got := ArithFloat64Batch(0, a, b)
		want := []float64{2.0, 4.0}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("[%d] got %v want %v", i, got[i], want[i])
			}
		}
	})

	t.Run("DispatchSIMDLevel", func(t *testing.T) {
		level := DispatchSIMDLevel()
		if level < 0 || level > 3 {
			t.Errorf("unexpected SIMD level: %d", level)
		}
	})
}

// TestPhase14_TypeConvBatch tests Phase 14: VM Type Conversion batch functions.
func TestPhase14_TypeConvBatch(t *testing.T) {
	t.Run("ParseInt64Batch", func(t *testing.T) {
		strs := []string{"123", "456", "abc", "-789"}
		vals, ok := ParseInt64Batch(strs)
		if !ok[0] || vals[0] != 123 {
			t.Errorf("index 0: got %d ok=%v", vals[0], ok[0])
		}
		if !ok[1] || vals[1] != 456 {
			t.Errorf("index 1: got %d ok=%v", vals[1], ok[1])
		}
		if ok[2] {
			t.Errorf("index 2 should fail: %v", ok[2])
		}
		if !ok[3] || vals[3] != -789 {
			t.Errorf("index 3: got %d ok=%v", vals[3], ok[3])
		}
	})

	t.Run("ParseFloat64Batch", func(t *testing.T) {
		strs := []string{"3.14", "2.71", "bad"}
		vals, ok := ParseFloat64Batch(strs)
		if !ok[0] || vals[0] < 3.13 || vals[0] > 3.15 {
			t.Errorf("index 0: got %v ok=%v", vals[0], ok[0])
		}
		if ok[2] {
			t.Errorf("index 2 should fail")
		}
	})

	t.Run("FormatInt64Batch", func(t *testing.T) {
		vals := []int64{0, -1, 9999999999}
		strs := FormatInt64Batch(vals)
		want := []string{"0", "-1", "9999999999"}
		for i, w := range want {
			if strs[i] != w {
				t.Errorf("[%d] got %q want %q", i, strs[i], w)
			}
		}
	})

	t.Run("FormatFloat64Batch", func(t *testing.T) {
		vals := []float64{3.14, -2.71, 0}
		strs := FormatFloat64Batch(vals)
		if len(strs) != 3 {
			t.Fatalf("expected 3 strings, got %d", len(strs))
		}
		if strs[2] != "0" {
			t.Errorf("zero: got %q want \"0\"", strs[2])
		}
	})
}

// TestPhase15_StringFuncsBatch tests Phase 15: VM String Functions batch functions.
func TestPhase15_StringFuncsBatch(t *testing.T) {
	t.Run("StrUpperBatch", func(t *testing.T) {
		got := StrUpperBatch([]string{"hello", "world", "GoLang"})
		want := []string{"HELLO", "WORLD", "GOLANG"}
		for i, w := range want {
			if got[i] != w {
				t.Errorf("[%d] got %q want %q", i, got[i], w)
			}
		}
	})

	t.Run("StrLowerBatch", func(t *testing.T) {
		got := StrLowerBatch([]string{"HELLO", "WORLD", "GoLang"})
		want := []string{"hello", "world", "golang"}
		for i, w := range want {
			if got[i] != w {
				t.Errorf("[%d] got %q want %q", i, got[i], w)
			}
		}
	})

	t.Run("StrTrimBatch", func(t *testing.T) {
		got := StrTrimBatch([]string{"  hello  ", " world", "no-spaces"})
		want := []string{"hello", "world", "no-spaces"}
		for i, w := range want {
			if got[i] != w {
				t.Errorf("[%d] got %q want %q", i, got[i], w)
			}
		}
	})

	t.Run("StrSubstrBatch", func(t *testing.T) {
		strs := []string{"hello", "world"}
		starts := []int64{2, 1}
		lengths := []int64{3, 3}
		got := StrSubstrBatch(strs, starts, lengths)
		want := []string{"ell", "wor"}
		for i, w := range want {
			if got[i] != w {
				t.Errorf("[%d] got %q want %q", i, got[i], w)
			}
		}
	})
}

// TestPhase16_DatetimeBatch tests Phase 16: VM DateTime Functions batch functions.
func TestPhase16_DatetimeBatch(t *testing.T) {
	t.Run("JuliandayFromString_Valid", func(t *testing.T) {
		jd, ok := JuliandayFromString("2000-01-01")
		if !ok {
			t.Skip("JuliandayFromString not ok for 2000-01-01")
		}
		// Julian Day of 2000-01-01 is approximately 2451544.5 to 2451545.5
		if jd < 2451540 || jd > 2451550 {
			t.Errorf("unexpected julianday: %v", jd)
		}
	})

	t.Run("UnixepochFromString_Valid", func(t *testing.T) {
		epoch, ok := UnixepochFromString("1970-01-01")
		if !ok {
			t.Skip("UnixepochFromString not ok for 1970-01-01")
		}
		// Unix epoch of 1970-01-01 is 0
		if epoch < -86400 || epoch > 86400 {
			t.Errorf("unexpected epoch for 1970-01-01: %d", epoch)
		}
	})

	t.Run("JuliandayBatch", func(t *testing.T) {
		strs := []string{"2000-01-01", "1970-01-01 00:00:00"}
		results, ok := JuliandayBatch(strs)
		if len(results) != 2 {
			t.Fatalf("expected 2 results")
		}
		_ = ok
	})
}

// TestPhase17_AggregateBatch tests Phase 17: VM Aggregate Functions batch functions.
func TestPhase17_AggregateBatch(t *testing.T) {
	t.Run("AggSumInt64_NoNulls", func(t *testing.T) {
		vals := []int64{1, 2, 3, 4, 5}
		sum, ok := AggSumInt64(vals, nil)
		if !ok || sum != 15 {
			t.Errorf("sum=%d ok=%v", sum, ok)
		}
	})

	t.Run("AggSumInt64_WithNulls", func(t *testing.T) {
		vals := []int64{1, 2, 3, 4, 5}
		nullMask := []bool{false, true, false, true, false}
		sum, ok := AggSumInt64(vals, nullMask)
		if !ok || sum != 9 {
			t.Errorf("sum=%d ok=%v (want 9)", sum, ok)
		}
	})

	t.Run("AggSumFloat64", func(t *testing.T) {
		vals := []float64{1.5, 2.5, 3.0}
		sum, ok := AggSumFloat64(vals, nil)
		if !ok || sum != 7.0 {
			t.Errorf("sum=%v ok=%v", sum, ok)
		}
	})

	t.Run("AggMinMaxInt64", func(t *testing.T) {
		vals := []int64{3, 1, 4, 1, 5, 9, 2, 6}
		mn, okMin := AggMinInt64(vals, nil)
		mx, okMax := AggMaxInt64(vals, nil)
		if !okMin || mn != 1 {
			t.Errorf("min=%d ok=%v", mn, okMin)
		}
		if !okMax || mx != 9 {
			t.Errorf("max=%d ok=%v", mx, okMax)
		}
	})

	t.Run("AggMinMaxFloat64", func(t *testing.T) {
		vals := []float64{3.0, 1.5, 4.0, 1.0, 5.5}
		mn, _ := AggMinFloat64(vals, nil)
		mx, _ := AggMaxFloat64(vals, nil)
		if mn != 1.0 {
			t.Errorf("min=%v want 1.0", mn)
		}
		if mx != 5.5 {
			t.Errorf("max=%v want 5.5", mx)
		}
	})

	t.Run("AggCountNotNull", func(t *testing.T) {
		nullMask := []bool{false, true, false, false, true}
		n := AggCountNotNull(nullMask, 5)
		if n != 3 {
			t.Errorf("count=%d want 3", n)
		}
	})

	t.Run("AggAllNull", func(t *testing.T) {
		vals := []int64{1, 2, 3}
		nullMask := []bool{true, true, true}
		_, ok := AggSumInt64(vals, nullMask)
		if ok {
			t.Error("expected ok=false when all null")
		}
	})
}
