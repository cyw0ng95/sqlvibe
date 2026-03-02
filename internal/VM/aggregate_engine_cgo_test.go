package VM

import (
	"testing"
)

// TestCAggregateEngine_CountSum tests basic COUNT and SUM aggregation.
func TestCAggregateEngine_CountSum(t *testing.T) {
	agg := NewCAggregateEngine()
	if agg == nil {
		t.Fatal("NewCAggregateEngine returned nil")
	}

	agg.Init()
	agg.SetGroupBy("g1")
	// Sum accumulation also counts rows (count is co-accumulated with sum).
	agg.AccumulateInt(AggFuncSum, 10)
	agg.AccumulateInt(AggFuncSum, 20)

	if got := agg.SumInt("g1"); got != 30 {
		t.Errorf("SumInt(g1) = %d; want 30", got)
	}
	// Each Sum call also increments count by 1.
	if got := agg.Count("g1"); got != 2 {
		t.Errorf("Count(g1) = %d; want 2", got)
	}
}

// TestCAggregateEngine_MultiGroup tests multiple independent groups.
func TestCAggregateEngine_MultiGroup(t *testing.T) {
	agg := NewCAggregateEngine()
	agg.Init()

	agg.SetGroupBy("a")
	agg.AccumulateInt(AggFuncSum, 5)
	agg.AccumulateInt(AggFuncSum, 5)

	agg.SetGroupBy("b")
	agg.AccumulateInt(AggFuncSum, 100)

	if got := agg.SumInt("a"); got != 10 {
		t.Errorf("SumInt(a) = %d; want 10", got)
	}
	if got := agg.SumInt("b"); got != 100 {
		t.Errorf("SumInt(b) = %d; want 100", got)
	}
}

// TestCAggregateEngine_FloatAvg tests float-based AVG aggregation.
func TestCAggregateEngine_FloatAvg(t *testing.T) {
	agg := NewCAggregateEngine()
	agg.Init()
	agg.SetGroupBy("x")
	// Avg accumulation co-accumulates sum and count.
	agg.AccumulateFloat(AggFuncAvg, 10.0)
	agg.AccumulateFloat(AggFuncAvg, 20.0)

	avg := agg.Avg("x")
	if avg < 14.0 || avg > 16.0 {
		t.Errorf("Avg(x) = %f; want ~15.0", avg)
	}
}

// TestCAggregateEngine_MinMax tests MIN and MAX aggregation.
func TestCAggregateEngine_MinMax(t *testing.T) {
	agg := NewCAggregateEngine()
	agg.Init()
	agg.SetGroupBy("m")
	for _, v := range []float64{3.0, 7.0, 1.0, 9.0, 4.0} {
		agg.AccumulateFloat(AggFuncMin, v)
		agg.AccumulateFloat(AggFuncMax, v)
	}
	if got := agg.Min("m"); got != 1.0 {
		t.Errorf("Min(m) = %f; want 1.0", got)
	}
	if got := agg.Max("m"); got != 9.0 {
		t.Errorf("Max(m) = %f; want 9.0", got)
	}
}

// TestCAggregateEngine_Reset tests that Reset clears accumulators.
func TestCAggregateEngine_Reset(t *testing.T) {
	agg := NewCAggregateEngine()
	agg.Init()
	agg.SetGroupBy("r")
	agg.AccumulateInt(AggFuncSum, 999)

	agg.Reset()
	if got := agg.SumInt("r"); got != 0 {
		t.Errorf("after Reset, SumInt(r) = %d; want 0", got)
	}
}
