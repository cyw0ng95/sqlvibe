//go:build !SVDB_ENABLE_CGO_VM
// +build !SVDB_ENABLE_CGO_VM

package VM

import "math"

// AggSumInt64 sums int64 values, skipping nulls (pure Go fallback).
// Returns sum and true, or 0 and false when all inputs are null.
func AggSumInt64(values []int64, nullMask []bool) (int64, bool) {
	var sum int64
	any := false
	for i, v := range values {
		if nullMask != nil && i < len(nullMask) && nullMask[i] {
			continue
		}
		sum += v
		any = true
	}
	return sum, any
}

// AggSumFloat64 sums float64 values (pure Go fallback).
func AggSumFloat64(values []float64, nullMask []bool) (float64, bool) {
	sum := 0.0
	any := false
	for i, v := range values {
		if nullMask != nil && i < len(nullMask) && nullMask[i] {
			continue
		}
		sum += v
		any = true
	}
	return sum, any
}

// AggMinInt64 finds the minimum int64 value (pure Go fallback).
func AggMinInt64(values []int64, nullMask []bool) (int64, bool) {
	mn := int64(math.MaxInt64)
	any := false
	for i, v := range values {
		if nullMask != nil && i < len(nullMask) && nullMask[i] {
			continue
		}
		if !any || v < mn {
			mn = v
			any = true
		}
	}
	return mn, any
}

// AggMaxInt64 finds the maximum int64 value (pure Go fallback).
func AggMaxInt64(values []int64, nullMask []bool) (int64, bool) {
	mx := int64(math.MinInt64)
	any := false
	for i, v := range values {
		if nullMask != nil && i < len(nullMask) && nullMask[i] {
			continue
		}
		if !any || v > mx {
			mx = v
			any = true
		}
	}
	return mx, any
}

// AggMinFloat64 finds the minimum float64 value (pure Go fallback).
func AggMinFloat64(values []float64, nullMask []bool) (float64, bool) {
	mn := math.MaxFloat64
	any := false
	for i, v := range values {
		if nullMask != nil && i < len(nullMask) && nullMask[i] {
			continue
		}
		if !any || v < mn {
			mn = v
			any = true
		}
	}
	return mn, any
}

// AggMaxFloat64 finds the maximum float64 value (pure Go fallback).
func AggMaxFloat64(values []float64, nullMask []bool) (float64, bool) {
	mx := -math.MaxFloat64
	any := false
	for i, v := range values {
		if nullMask != nil && i < len(nullMask) && nullMask[i] {
			continue
		}
		if !any || v > mx {
			mx = v
			any = true
		}
	}
	return mx, any
}

// AggCountNotNull counts non-null values (pure Go fallback).
func AggCountNotNull(nullMask []bool, total int) int64 {
	if nullMask == nil {
		return int64(total)
	}
	var n int64
	for i := 0; i < total && i < len(nullMask); i++ {
		if !nullMask[i] {
			n++
		}
	}
	return n
}
