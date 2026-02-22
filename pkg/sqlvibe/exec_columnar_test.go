package sqlvibe

import (
	"testing"

	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe/storage"
)

// buildIntCol builds a TypeInt ColumnVector from a slice of int64 values.
// A value of math.MinInt64 is treated as NULL for test convenience.
func buildIntCol(vals []int64, nulls []bool) *storage.ColumnVector {
	cv := storage.NewColumnVector("v", storage.TypeInt)
	for i, v := range vals {
		if nulls != nil && i < len(nulls) && nulls[i] {
			cv.AppendNull()
		} else {
			cv.Append(storage.IntValue(v))
		}
	}
	return cv
}

func buildFloatCol(vals []float64, nulls []bool) *storage.ColumnVector {
	cv := storage.NewColumnVector("v", storage.TypeFloat)
	for i, v := range vals {
		if nulls != nil && i < len(nulls) && nulls[i] {
			cv.AppendNull()
		} else {
			cv.Append(storage.FloatValue(v))
		}
	}
	return cv
}

func buildStringCol(vals []string, nulls []bool) *storage.ColumnVector {
	cv := storage.NewColumnVector("v", storage.TypeString)
	for i, v := range vals {
		if nulls != nil && i < len(nulls) && nulls[i] {
			cv.AppendNull()
		} else {
			cv.Append(storage.StringValue(v))
		}
	}
	return cv
}

// ---- VectorizedFilter tests ----

func TestVectorizedFilter_EqualInt(t *testing.T) {
	col := buildIntCol([]int64{1, 2, 3, 2, 5}, nil)
	rb := VectorizedFilter(col, "=", storage.IntValue(2))
	got := rb.ToSlice()
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("expected [1 3], got %v", got)
	}
}

func TestVectorizedFilter_NotEqual(t *testing.T) {
	col := buildIntCol([]int64{1, 2, 3}, nil)
	rb := VectorizedFilter(col, "!=", storage.IntValue(2))
	got := rb.ToSlice()
	if len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("expected [0 2], got %v", got)
	}
}

func TestVectorizedFilter_LessThan(t *testing.T) {
	col := buildIntCol([]int64{10, 5, 20, 3}, nil)
	rb := VectorizedFilter(col, "<", storage.IntValue(10))
	got := rb.ToSlice()
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("expected [1 3], got %v", got)
	}
}

func TestVectorizedFilter_LessEqual(t *testing.T) {
	col := buildIntCol([]int64{10, 5, 20, 10}, nil)
	rb := VectorizedFilter(col, "<=", storage.IntValue(10))
	got := rb.ToSlice()
	if len(got) != 3 {
		t.Fatalf("expected 3 results, got %v", got)
	}
}

func TestVectorizedFilter_GreaterThan(t *testing.T) {
	col := buildIntCol([]int64{1, 5, 3, 7}, nil)
	rb := VectorizedFilter(col, ">", storage.IntValue(4))
	got := rb.ToSlice()
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("expected [1 3], got %v", got)
	}
}

func TestVectorizedFilter_GreaterEqual(t *testing.T) {
	col := buildIntCol([]int64{1, 5, 3, 5}, nil)
	rb := VectorizedFilter(col, ">=", storage.IntValue(5))
	got := rb.ToSlice()
	if len(got) != 2 || got[0] != 1 || got[1] != 3 {
		t.Fatalf("expected [1 3], got %v", got)
	}
}

func TestVectorizedFilter_SkipsNulls(t *testing.T) {
	col := buildIntCol([]int64{1, 0, 3}, []bool{false, true, false})
	rb := VectorizedFilter(col, ">", storage.IntValue(0))
	got := rb.ToSlice()
	// index 1 is null, should be skipped
	if len(got) != 2 || got[0] != 0 || got[1] != 2 {
		t.Fatalf("expected [0 2], got %v", got)
	}
}

func TestVectorizedFilter_String(t *testing.T) {
	col := buildStringCol([]string{"apple", "banana", "cherry"}, nil)
	rb := VectorizedFilter(col, "=", storage.StringValue("banana"))
	got := rb.ToSlice()
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("expected [1], got %v", got)
	}
}

func TestVectorizedFilter_Float(t *testing.T) {
	col := buildFloatCol([]float64{1.1, 2.2, 3.3}, nil)
	rb := VectorizedFilter(col, ">", storage.FloatValue(2.0))
	got := rb.ToSlice()
	if len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("expected [1 2], got %v", got)
	}
}

// ---- Aggregation tests ----

func TestColumnarSum(t *testing.T) {
	col := buildIntCol([]int64{1, 2, 3, 4, 5}, nil)
	if got := ColumnarSum(col); got != 15 {
		t.Fatalf("expected 15, got %v", got)
	}
}

func TestColumnarSum_SkipsNulls(t *testing.T) {
	col := buildIntCol([]int64{10, 0, 20}, []bool{false, true, false})
	if got := ColumnarSum(col); got != 30 {
		t.Fatalf("expected 30, got %v", got)
	}
}

func TestColumnarSum_Float(t *testing.T) {
	col := buildFloatCol([]float64{1.5, 2.5, 1.0}, nil)
	if got := ColumnarSum(col); got != 5.0 {
		t.Fatalf("expected 5.0, got %v", got)
	}
}

func TestColumnarCount(t *testing.T) {
	col := buildIntCol([]int64{1, 0, 3}, []bool{false, true, false})
	if got := ColumnarCount(col); got != 2 {
		t.Fatalf("expected 2, got %v", got)
	}
}

func TestColumnarCount_AllNull(t *testing.T) {
	col := buildIntCol([]int64{0, 0}, []bool{true, true})
	if got := ColumnarCount(col); got != 0 {
		t.Fatalf("expected 0, got %v", got)
	}
}

func TestColumnarMin(t *testing.T) {
	col := buildIntCol([]int64{5, 2, 8, 1, 4}, nil)
	v := ColumnarMin(col)
	if v.Int != 1 {
		t.Fatalf("expected 1, got %v", v)
	}
}

func TestColumnarMin_AllNull(t *testing.T) {
	col := buildIntCol([]int64{0}, []bool{true})
	v := ColumnarMin(col)
	if !v.IsNull() {
		t.Fatal("expected NULL for all-null column")
	}
}

func TestColumnarMin_String(t *testing.T) {
	col := buildStringCol([]string{"banana", "apple", "cherry"}, nil)
	v := ColumnarMin(col)
	if v.Str != "apple" {
		t.Fatalf("expected apple, got %v", v.Str)
	}
}

func TestColumnarMax(t *testing.T) {
	col := buildIntCol([]int64{5, 2, 8, 1, 4}, nil)
	v := ColumnarMax(col)
	if v.Int != 8 {
		t.Fatalf("expected 8, got %v", v)
	}
}

func TestColumnarMax_AllNull(t *testing.T) {
	col := buildIntCol([]int64{0}, []bool{true})
	v := ColumnarMax(col)
	if !v.IsNull() {
		t.Fatal("expected NULL for all-null column")
	}
}

func TestColumnarAvg(t *testing.T) {
	col := buildIntCol([]int64{2, 4, 6}, nil)
	got, ok := ColumnarAvg(col)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got != 4.0 {
		t.Fatalf("expected 4.0, got %v", got)
	}
}

func TestColumnarAvg_SkipsNulls(t *testing.T) {
	col := buildIntCol([]int64{10, 0, 20}, []bool{false, true, false})
	got, ok := ColumnarAvg(col)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got != 15.0 {
		t.Fatalf("expected 15.0, got %v", got)
	}
}

func TestColumnarAvg_NoValues(t *testing.T) {
	col := buildIntCol([]int64{0}, []bool{true})
	_, ok := ColumnarAvg(col)
	if ok {
		t.Fatal("expected ok=false for all-null column")
	}
}

// ---- ColumnarGroupBy tests ----

func TestColumnarGroupBy_Sum(t *testing.T) {
	keys := buildStringCol([]string{"a", "b", "a", "b", "a"}, nil)
	vals := buildIntCol([]int64{1, 10, 2, 20, 3}, nil)
	res := ColumnarGroupBy(keys, vals, "sum")
	if res["a"].Float != 6 {
		t.Fatalf("expected a=6, got %v", res["a"])
	}
	if res["b"].Float != 30 {
		t.Fatalf("expected b=30, got %v", res["b"])
	}
}

func TestColumnarGroupBy_Count(t *testing.T) {
	keys := buildStringCol([]string{"x", "y", "x"}, nil)
	vals := buildIntCol([]int64{1, 2, 3}, nil)
	res := ColumnarGroupBy(keys, vals, "count")
	if res["x"].Int != 2 {
		t.Fatalf("expected x=2, got %v", res["x"])
	}
	if res["y"].Int != 1 {
		t.Fatalf("expected y=1, got %v", res["y"])
	}
}

func TestColumnarGroupBy_Min(t *testing.T) {
	keys := buildStringCol([]string{"a", "a", "b"}, nil)
	vals := buildIntCol([]int64{5, 3, 7}, nil)
	res := ColumnarGroupBy(keys, vals, "min")
	if res["a"].Int != 3 {
		t.Fatalf("expected a=3, got %v", res["a"])
	}
	if res["b"].Int != 7 {
		t.Fatalf("expected b=7, got %v", res["b"])
	}
}

func TestColumnarGroupBy_Max(t *testing.T) {
	keys := buildStringCol([]string{"a", "a", "b"}, nil)
	vals := buildIntCol([]int64{5, 3, 7}, nil)
	res := ColumnarGroupBy(keys, vals, "max")
	if res["a"].Int != 5 {
		t.Fatalf("expected a=5, got %v", res["a"])
	}
}

func TestColumnarGroupBy_Avg(t *testing.T) {
	keys := buildStringCol([]string{"g", "g", "g"}, nil)
	vals := buildIntCol([]int64{3, 6, 9}, nil)
	res := ColumnarGroupBy(keys, vals, "avg")
	if res["g"].Float != 6.0 {
		t.Fatalf("expected g=6.0, got %v", res["g"])
	}
}

func TestColumnarGroupBy_SkipsNullKeys(t *testing.T) {
	keys := buildStringCol([]string{"a", "", "a"}, []bool{false, true, false})
	vals := buildIntCol([]int64{1, 100, 2}, nil)
	res := ColumnarGroupBy(keys, vals, "sum")
	if _, ok := res[""]; ok {
		t.Fatal("null key should not appear in result")
	}
	if res["a"].Float != 3 {
		t.Fatalf("expected a=3, got %v", res["a"])
	}
}
