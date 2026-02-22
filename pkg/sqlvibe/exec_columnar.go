package sqlvibe

import (
	"strings"

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

// ColumnarHashJoin performs an inner join between left and right stores on a single
// column pair (leftCol = rightCol).  The result is a slice of merged value rows
// where the first len(left.Columns()) values are from the left row and the
// remaining values are from the right row.
func ColumnarHashJoin(left, right *storage.HybridStore, leftCol, rightCol string) [][]storage.Value {
	lci := left.ColIndex(leftCol)
	rci := right.ColIndex(rightCol)
	if lci < 0 || rci < 0 {
		return nil
	}

	// Build hash table from the smaller side (right).
	hash := make(map[string][][]storage.Value)
	for _, rRow := range right.Scan() {
		key := rRow[rci].String()
		hash[key] = append(hash[key], rRow)
	}

	// Probe with left side.
	var out [][]storage.Value
	lCols := len(left.Columns())
	rCols := len(right.Columns())
	for _, lRow := range left.Scan() {
		key := lRow[lci].String()
		matches, ok := hash[key]
		if !ok {
			continue
		}
		for _, rRow := range matches {
			merged := make([]storage.Value, lCols+rCols)
			copy(merged[:lCols], lRow)
			copy(merged[lCols:], rRow)
			out = append(out, merged)
		}
	}
	return out
}

// VectorizedGroupBy groups the rows in hs by the values of groupCols and computes
// aggregate functions on aggCol.  agg may be "sum", "count", "min", "max", or "avg".
// Each returned row contains the group-key values (in the order of groupCols)
// followed by the aggregate result.
func VectorizedGroupBy(hs *storage.HybridStore, groupCols []string, aggCol, agg string) [][]storage.Value {
	colIdx := make([]int, len(groupCols))
	for i, c := range groupCols {
		colIdx[i] = hs.ColIndex(c)
	}
	aggCI := hs.ColIndex(aggCol)

	type aggState struct {
		keyVals []storage.Value // representative key values for this group
		sum     float64
		count   int64
		min     storage.Value
		max     storage.Value
		hasVal  bool
	}

	groups := make(map[string]*aggState)
	keyOrder := make([]string, 0)

	for _, row := range hs.Scan() {
		// Build composite group key.
		var kb strings.Builder
		for i, ci := range colIdx {
			if ci >= 0 && ci < len(row) {
				kb.WriteString(row[ci].String())
			}
			if i < len(colIdx)-1 {
				kb.WriteByte(0) // null separator
			}
		}
		key := kb.String()

		gs, ok := groups[key]
		if !ok {
			// Store representative key values from the first row in this group.
			keyVals := make([]storage.Value, len(groupCols))
			for i, ci := range colIdx {
				if ci >= 0 && ci < len(row) {
					keyVals[i] = row[ci]
				}
			}
			gs = &aggState{keyVals: keyVals}
			groups[key] = gs
			keyOrder = append(keyOrder, key)
		}

		if agg == "count" {
			gs.count++
			continue
		}

		var v storage.Value
		if aggCI >= 0 && aggCI < len(row) {
			v = row[aggCI]
		}
		if v.IsNull() {
			continue
		}
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

	out := make([][]storage.Value, 0, len(keyOrder))
	for _, key := range keyOrder {
		gs := groups[key]

		var aggVal storage.Value
		switch agg {
		case "sum":
			aggVal = storage.FloatValue(gs.sum)
		case "count":
			aggVal = storage.IntValue(gs.count)
		case "min":
			if gs.hasVal {
				aggVal = gs.min
			} else {
				aggVal = storage.NullValue()
			}
		case "max":
			if gs.hasVal {
				aggVal = gs.max
			} else {
				aggVal = storage.NullValue()
			}
		case "avg":
			if gs.count == 0 {
				aggVal = storage.NullValue()
			} else {
				aggVal = storage.FloatValue(gs.sum / float64(gs.count))
			}
		default:
			aggVal = storage.NullValue()
		}

		row := append(gs.keyVals, aggVal)
		out = append(out, row)
	}
	return out
}
