package engine

import "fmt"

// GroupRows partitions rows into groups using keyFn.  All rows that produce the
// same key are placed in the same group.  Order within each group is preserved.
func GroupRows(rows []Row, keyFn func(Row) string) map[string][]Row {
	groups := make(map[string][]Row)
	for _, r := range rows {
		k := keyFn(r)
		groups[k] = append(groups[k], r)
	}
	return groups
}

// CountRows counts the number of non-nil values in col across all rows.
// If col is empty, every row is counted regardless of content.
func CountRows(rows []Row, col string) int64 {
	var n int64
	for _, r := range rows {
		if col == "" {
			n++
			continue
		}
		if v, ok := r[col]; ok && v != nil {
			n++
		}
	}
	return n
}

// SumRows returns the sum of numeric values in col.  Non-numeric and nil
// values are ignored.  Returns nil if there are no non-nil values.
func SumRows(rows []Row, col string) interface{} {
	var sum float64
	var count int
	for _, r := range rows {
		v, ok := r[col]
		if !ok || v == nil {
			continue
		}
		f, ok := toFloat(v)
		if !ok {
			continue
		}
		sum += f
		count++
	}
	if count == 0 {
		return nil
	}
	// If all values were integer, return float64 (SQLite SUM behaviour).
	return sum
}

// AvgRows returns the arithmetic mean of numeric values in col.
// Returns nil if there are no non-nil values.
func AvgRows(rows []Row, col string) interface{} {
	var sum float64
	var count int
	for _, r := range rows {
		v, ok := r[col]
		if !ok || v == nil {
			continue
		}
		f, ok := toFloat(v)
		if !ok {
			continue
		}
		sum += f
		count++
	}
	if count == 0 {
		return nil
	}
	return sum / float64(count)
}

// MinRows returns the minimum value in col using cmp.
// Returns nil if there are no non-nil values.
func MinRows(rows []Row, col string, cmp func(a, b interface{}) int) interface{} {
	var min interface{}
	for _, r := range rows {
		v, ok := r[col]
		if !ok || v == nil {
			continue
		}
		if min == nil || cmp(v, min) < 0 {
			min = v
		}
	}
	return min
}

// MaxRows returns the maximum value in col using cmp.
// Returns nil if there are no non-nil values.
func MaxRows(rows []Row, col string, cmp func(a, b interface{}) int) interface{} {
	var max interface{}
	for _, r := range rows {
		v, ok := r[col]
		if !ok || v == nil {
			continue
		}
		if max == nil || cmp(v, max) > 0 {
			max = v
		}
	}
	return max
}

// GroupByAndAggregate groups rows by keyFn, then applies aggFn to each group,
// returning one result row per group.  aggFn is called with the group key and
// all rows in that group; it returns the output row for the group.
func GroupByAndAggregate(rows []Row, keyFn func(Row) string, aggFn func(key string, group []Row) Row) []Row {
	// Preserve insertion order for deterministic output.
	type entry struct {
		key  string
		rows []Row
	}
	var order []string
	groups := make(map[string]*entry)
	for _, r := range rows {
		k := keyFn(r)
		if e, ok := groups[k]; ok {
			e.rows = append(e.rows, r)
		} else {
			order = append(order, k)
			groups[k] = &entry{key: k, rows: []Row{r}}
		}
	}
	out := make([]Row, 0, len(order))
	for _, k := range order {
		out = append(out, aggFn(k, groups[k].rows))
	}
	return out
}

// toFloat converts a numeric interface value to float64.
func toFloat(v interface{}) (float64, bool) {
	switch x := v.(type) {
	case int64:
		return float64(x), true
	case int:
		return float64(x), true
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case string:
		var f float64
		_, err := fmt.Sscanf(x, "%g", &f)
		return f, err == nil
	}
	return 0, false
}
