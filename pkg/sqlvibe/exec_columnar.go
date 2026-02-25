package sqlvibe

import (
	"context"
	"strings"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
)

// VectorizedFilter applies op against val across all non-null rows of col and
// returns a RoaringBitmap of matching row indices.
// Supported ops: "=", "!=", "<", "<=", ">", ">="
func VectorizedFilter(col *DS.ColumnVector, op string, val DS.Value) *DS.RoaringBitmap {
	rb := DS.NewRoaringBitmap()
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		cmp := DS.Compare(col.Get(i), val)
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
func ColumnarSum(col *DS.ColumnVector) float64 {
	var sum float64
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		v := col.Get(i)
		switch v.Type {
		case DS.TypeInt:
			sum += float64(v.Int)
		case DS.TypeFloat:
			sum += v.Float
		}
	}
	return sum
}

// ColumnarCount returns the number of non-null values in col.
func ColumnarCount(col *DS.ColumnVector) int64 {
	var n int64
	for i := 0; i < col.Len(); i++ {
		if !col.IsNull(i) {
			n++
		}
	}
	return n
}

// ColumnarMin returns the minimum non-null value, or NullValue if the column is empty/all-null.
func ColumnarMin(col *DS.ColumnVector) DS.Value {
	min := DS.NullValue()
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		v := col.Get(i)
		if min.IsNull() || DS.Compare(v, min) < 0 {
			min = v
		}
	}
	return min
}

// ColumnarMax returns the maximum non-null value, or NullValue if the column is empty/all-null.
func ColumnarMax(col *DS.ColumnVector) DS.Value {
	max := DS.NullValue()
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		v := col.Get(i)
		if max.IsNull() || DS.Compare(v, max) > 0 {
			max = v
		}
	}
	return max
}

// ColumnarAvg returns the average of all non-null TypeInt and TypeFloat values,
// and whether any qualifying values existed (false means no data / all NULL).
func ColumnarAvg(col *DS.ColumnVector) (float64, bool) {
	var sum float64
	var count int64
	for i := 0; i < col.Len(); i++ {
		if col.IsNull(i) {
			continue
		}
		v := col.Get(i)
		switch v.Type {
		case DS.TypeInt:
			sum += float64(v.Int)
			count++
		case DS.TypeFloat:
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
	min    DS.Value
	max    DS.Value
	hasVal bool
}

// ColumnarGroupBy groups rows by keyCol values and aggregates valCol values.
// agg may be "sum", "count", "min", "max", or "avg".
// The returned map key is the string representation of each group key value.
// Rows where keyCol is null are skipped.
func ColumnarGroupBy(keyCol, valCol *DS.ColumnVector, agg string) map[string]DS.Value {
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
		case DS.TypeInt:
			gs.sum += float64(v.Int)
		case DS.TypeFloat:
			gs.sum += v.Float
		}
		if !gs.hasVal {
			gs.min = v
			gs.max = v
			gs.hasVal = true
		} else {
			if DS.Compare(v, gs.min) < 0 {
				gs.min = v
			}
			if DS.Compare(v, gs.max) > 0 {
				gs.max = v
			}
		}
	}

	result := make(map[string]DS.Value, len(groups))
	for _, key := range keyOrder {
		gs := groups[key]
		switch agg {
		case "sum":
			result[key] = DS.FloatValue(gs.sum)
		case "count":
			result[key] = DS.IntValue(gs.count)
		case "min":
			if gs.hasVal {
				result[key] = gs.min
			} else {
				result[key] = DS.NullValue()
			}
		case "max":
			if gs.hasVal {
				result[key] = gs.max
			} else {
				result[key] = DS.NullValue()
			}
		case "avg":
			if gs.count == 0 {
				result[key] = DS.NullValue()
			} else {
				result[key] = DS.FloatValue(gs.sum / float64(gs.count))
			}
		default:
			result[key] = DS.NullValue()
		}
	}
	return result
}

// joinHashKey returns a hashable key for a DS.Value without allocation for the
// common int64/float64/string cases.  For rare types (bytes, etc.) it falls
// back to the string representation.
func joinHashKey(v DS.Value) interface{} {
	switch v.Type {
	case DS.TypeInt, DS.TypeBool:
		return v.Int
	case DS.TypeFloat:
		return v.Float
	case DS.TypeString:
		return v.Str
	default:
		return v.String()
	}
}

// ColumnarHashJoin performs an inner join between left and right stores on a single
// column pair (leftCol = rightCol).  The result is a slice of merged value rows
// where the first len(left.Columns()) values are from the left row and the
// remaining values are from the right row.
func ColumnarHashJoin(left, right *DS.HybridStore, leftCol, rightCol string) [][]DS.Value {
	return ColumnarHashJoinContext(context.Background(), left, right, leftCol, rightCol)
}

// ColumnarHashJoinContext is a context-aware variant of ColumnarHashJoin.
// It checks ctx.Done() every 256 rows in the build phase and returns nil on cancellation.
func ColumnarHashJoinContext(ctx context.Context, left, right *DS.HybridStore, leftCol, rightCol string) [][]DS.Value {
	lci := left.ColIndex(leftCol)
	rci := right.ColIndex(rightCol)
	if lci < 0 || rci < 0 {
		return nil
	}

	// Build hash table from the smaller side (right).
	// Use interface{} key to avoid fmt.Sprintf allocation for int/float/string values.
	hash := make(map[interface{}][][]DS.Value)
	var buildCount int64
	for _, rRow := range right.Scan() {
		key := joinHashKey(rRow[rci])
		hash[key] = append(hash[key], rRow)
		buildCount++
		if buildCount%256 == 0 {
			select {
			case <-ctx.Done():
				return nil
			default:
			}
		}
	}

	// Probe with left side.
	var out [][]DS.Value
	lCols := len(left.Columns())
	rCols := len(right.Columns())
	for _, lRow := range left.Scan() {
		key := joinHashKey(lRow[lci])
		matches, ok := hash[key]
		if !ok {
			continue
		}
		for _, rRow := range matches {
			merged := make([]DS.Value, lCols+rCols)
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
func VectorizedGroupBy(hs *DS.HybridStore, groupCols []string, aggCol, agg string) [][]DS.Value {
	colIdx := make([]int, len(groupCols))
	for i, c := range groupCols {
		colIdx[i] = hs.ColIndex(c)
	}
	aggCI := hs.ColIndex(aggCol)

	type aggState struct {
		keyVals []DS.Value // representative key values for this group
		sum     float64
		count   int64
		min     DS.Value
		max     DS.Value
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
			keyVals := make([]DS.Value, len(groupCols))
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

		var v DS.Value
		if aggCI >= 0 && aggCI < len(row) {
			v = row[aggCI]
		}
		if v.IsNull() {
			continue
		}
		gs.count++
		switch v.Type {
		case DS.TypeInt:
			gs.sum += float64(v.Int)
		case DS.TypeFloat:
			gs.sum += v.Float
		}
		if !gs.hasVal {
			gs.min = v
			gs.max = v
			gs.hasVal = true
		} else {
			if DS.Compare(v, gs.min) < 0 {
				gs.min = v
			}
			if DS.Compare(v, gs.max) > 0 {
				gs.max = v
			}
		}
	}

	out := make([][]DS.Value, 0, len(keyOrder))
	for _, key := range keyOrder {
		gs := groups[key]

		var aggVal DS.Value
		switch agg {
		case "sum":
			aggVal = DS.FloatValue(gs.sum)
		case "count":
			aggVal = DS.IntValue(gs.count)
		case "min":
			if gs.hasVal {
				aggVal = gs.min
			} else {
				aggVal = DS.NullValue()
			}
		case "max":
			if gs.hasVal {
				aggVal = gs.max
			} else {
				aggVal = DS.NullValue()
			}
		case "avg":
			if gs.count == 0 {
				aggVal = DS.NullValue()
			} else {
				aggVal = DS.FloatValue(gs.sum / float64(gs.count))
			}
		default:
			aggVal = DS.NullValue()
		}

		row := append(gs.keyVals, aggVal)
		out = append(out, row)
	}
	return out
}
