package sqlvibe

import (
	"github.com/sqlvibe/sqlvibe/pkg/sqlvibe/storage"
)

// VectorizedFilter applies op against val across all non-null rows of col and
// returns a RoaringBitmap of matching row indices.
// Supported ops: "=", "!=", "<", "<=", ">", ">="
func VectorizedFilter(col *storage.ColumnVector, op string, val storage.Value) *storage.RoaringBitmap {
	rb := storage.NewRoaringBitmap()
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		cmp := storage.Compare(col.Get(i), val)
		var match bool
		switch op {
		case "=":
			match = cmp == 0
		case "!=":
			match = cmp != 0
		case "<":
			match = cmp < 0
		case "<=":
			match = cmp <= 0
		case ">":
			match = cmp > 0
		case ">=":
			match = cmp >= 0
		}
		if match {
			rb.Add(uint32(i))
		}
	}
	return rb
}

// ColumnarSum returns the sum of all non-null TypeInt and TypeFloat values in col.
func ColumnarSum(col *storage.ColumnVector) float64 {
	var sum float64
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		v := col.Get(i)
		switch v.Type {
		case storage.TypeInt:
			sum += float64(v.Int)
		case storage.TypeFloat:
			sum += v.Float
		}
	}
	return sum
}

// ColumnarCount returns the number of non-null values in col.
func ColumnarCount(col *storage.ColumnVector) int64 {
	var n int64
	for i := 0; i < col.Len(); i++ {
		if !col.IsNull(i) {
			n++
		}
	}
	return n
}

// ColumnarMin returns the minimum non-null value, or NullValue if the column is empty/all-null.
func ColumnarMin(col *storage.ColumnVector) storage.Value {
	min := storage.NullValue()
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		v := col.Get(i)
		if min.IsNull() || storage.Compare(v, min) < 0 {
			min = v
		}
	}
	return min
}

// ColumnarMax returns the maximum non-null value, or NullValue if the column is empty/all-null.
func ColumnarMax(col *storage.ColumnVector) storage.Value {
	max := storage.NullValue()
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		v := col.Get(i)
		if max.IsNull() || storage.Compare(v, max) > 0 {
			max = v
		}
	}
	return max
}

// ColumnarAvg returns the average of all non-null TypeInt and TypeFloat values,
// and whether any qualifying values existed (false means no data / all NULL).
func ColumnarAvg(col *storage.ColumnVector) (float64, bool) {
	var sum float64
	var count int64
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		v := col.Get(i)
		switch v.Type {
		case storage.TypeInt:
			sum += float64(v.Int)
			count++
		case storage.TypeFloat:
			sum += v.Float
			count++
		}
	}
	if count == 0 {
		return 0, false
	}
	return sum / float64(count), true
}

// groupState holds per-group aggregation accumulators.
type groupState struct {
	sum    float64
	count  int64
	min    storage.Value
	max    storage.Value
	hasVal bool
}

// ColumnarGroupBy groups rows by keyCol values and aggregates valCol values.
// agg may be "sum", "count", "min", "max", or "avg".
// The returned map key is the string representation of each group key value.
// Rows where keyCol is null are skipped.
func ColumnarGroupBy(keyCol, valCol *storage.ColumnVector, agg string) map[string]storage.Value {
	groups := make(map[string]*groupState)
	keyOrder := make([]string, 0)

	n := keyCol.Len()
	for i := 0; i < n; i++ {
		if keyCol.IsNull(i) {
			continue
		}
		key := keyCol.Get(i).String()
		if _, ok := groups[key]; !ok {
			groups[key] = &groupState{}
			keyOrder = append(keyOrder, key)
		}
		gs := groups[key]

		if agg == "count" {
			gs.count++
			continue
		}

		if valCol == nil || i >= valCol.Len() || valCol.IsNull(i) {
			continue
		}
		v := valCol.Get(i)
		gs.count++
		switch v.Type {
		case storage.TypeInt:
			gs.sum += float64(v.Int)
		case storage.TypeFloat:
			gs.sum += v.Float
		}
		if !gs.hasVal {
			gs.min = v
			gs.max = v
			gs.hasVal = true
		} else {
			if storage.Compare(v, gs.min) < 0 {
				gs.min = v
			}
			if storage.Compare(v, gs.max) > 0 {
				gs.max = v
			}
		}
	}

	result := make(map[string]storage.Value, len(groups))
	for _, key := range keyOrder {
		gs := groups[key]
		switch agg {
		case "sum":
			result[key] = storage.FloatValue(gs.sum)
		case "count":
			result[key] = storage.IntValue(gs.count)
		case "min":
			if gs.hasVal {
				result[key] = gs.min
			} else {
				result[key] = storage.NullValue()
			}
		case "max":
			if gs.hasVal {
				result[key] = gs.max
			} else {
				result[key] = storage.NullValue()
			}
		case "avg":
			if gs.count == 0 {
				result[key] = storage.NullValue()
			} else {
				result[key] = storage.FloatValue(gs.sum / float64(gs.count))
			}
		default:
			result[key] = storage.NullValue()
		}
	}
	return result
}
