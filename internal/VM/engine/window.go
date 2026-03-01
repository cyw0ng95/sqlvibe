package engine

// PartitionRows splits rows into partitions using partitionFn.  All rows that
// produce the same key are placed in the same partition.  The order of
// partitions matches the first occurrence of each key in rows.
func PartitionRows(rows []Row, partitionFn func(Row) string) [][]Row {
	type entry struct {
		key  string
		rows []Row
	}
	var order []string
	groups := make(map[string]*entry)
	for _, r := range rows {
		k := partitionFn(r)
		if e, ok := groups[k]; ok {
			e.rows = append(e.rows, r)
		} else {
			order = append(order, k)
			groups[k] = &entry{key: k, rows: []Row{r}}
		}
	}
	out := make([][]Row, 0, len(order))
	for _, k := range order {
		out = append(out, groups[k].rows)
	}
	return out
}

// RowNumbers returns a slice of sequential row numbers (1-based) for each row.
func RowNumbers(rows []Row) []int64 {
	out := make([]int64, len(rows))
	for i := range rows {
		out[i] = int64(i + 1)
	}
	return out
}

// Ranks returns RANK() values.  Rows that are equal under less (i.e. neither
// less(a,b) nor less(b,a)) share the same rank; the next distinct rank skips
// positions accordingly.
func Ranks(rows []Row, less func(a, b Row) bool) []int64 {
	out := make([]int64, len(rows))
	if len(rows) == 0 {
		return out
	}
	rank := int64(1)
	for i := range rows {
		if i == 0 {
			out[i] = rank
			continue
		}
		// Advance rank by number of tied rows above.
		if less(rows[i-1], rows[i]) || less(rows[i], rows[i-1]) {
			rank = int64(i + 1)
		}
		out[i] = rank
	}
	return out
}

// DenseRanks returns DENSE_RANK() values.  Equal rows share a rank; the next
// distinct value gets rank+1 (no gaps).
func DenseRanks(rows []Row, less func(a, b Row) bool) []int64 {
	out := make([]int64, len(rows))
	if len(rows) == 0 {
		return out
	}
	rank := int64(1)
	for i := range rows {
		if i == 0 {
			out[i] = rank
			continue
		}
		if less(rows[i-1], rows[i]) || less(rows[i], rows[i-1]) {
			rank++
		}
		out[i] = rank
	}
	return out
}

// LagValues returns LAG(colFn, n, defaultVal) values.  Each position i gets
// colFn(rows[i-n]) if i-n >= 0, otherwise defaultVal.
func LagValues(rows []Row, colFn func(Row) interface{}, n int, defaultVal interface{}) []interface{} {
	if n <= 0 {
		n = 1
	}
	out := make([]interface{}, len(rows))
	for i := range rows {
		if i-n >= 0 {
			out[i] = colFn(rows[i-n])
		} else {
			out[i] = defaultVal
		}
	}
	return out
}

// LeadValues returns LEAD(colFn, n, defaultVal) values.  Each position i gets
// colFn(rows[i+n]) if i+n < len(rows), otherwise defaultVal.
func LeadValues(rows []Row, colFn func(Row) interface{}, n int, defaultVal interface{}) []interface{} {
	if n <= 0 {
		n = 1
	}
	out := make([]interface{}, len(rows))
	for i := range rows {
		if i+n < len(rows) {
			out[i] = colFn(rows[i+n])
		} else {
			out[i] = defaultVal
		}
	}
	return out
}

// NthValues returns the nth value of colFn within the window frame [0, i].
// n is 1-based.  For positions where i < n-1 (the frame doesn't yet include
// the nth element), nil is returned.  Once the frame reaches or exceeds n
// elements, rows[n-1] is returned for all subsequent positions.
func NthValues(rows []Row, colFn func(Row) interface{}, n int) []interface{} {
	out := make([]interface{}, len(rows))
	if n <= 0 {
		return out
	}
	for i := range rows {
		if i >= n-1 {
			// The nth element (1-based) is at index n-1 in the sorted partition.
			out[i] = colFn(rows[n-1])
		}
		// else out[i] remains nil: frame [0,i] has fewer than n elements
	}
	return out
}

// FirstValues returns FIRST_VALUE(colFn) within each window frame [0, i].
func FirstValues(rows []Row, colFn func(Row) interface{}) []interface{} {
	return NthValues(rows, colFn, 1)
}

// LastValues returns LAST_VALUE(colFn) for each position i (value at i itself
// under the default frame ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW).
func LastValues(rows []Row, colFn func(Row) interface{}) []interface{} {
	out := make([]interface{}, len(rows))
	for i := range rows {
		out[i] = colFn(rows[i])
	}
	return out
}
