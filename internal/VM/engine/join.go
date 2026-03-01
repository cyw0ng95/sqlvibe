package engine

// MergeRows merges two Row maps into a new Row.  Values from b overwrite
// values from a when both share the same key.
func MergeRows(a, b Row) Row {
	out := make(Row, len(a)+len(b))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
}

// MergeRowsWithAlias merges two rows, adding alias-qualified keys for each
// source.  For example, with aliasA="e" and aliasB="d", column "id" from a
// is stored as both "id" and "e.id" in the result.  Values from b take
// precedence over a for unqualified keys.
func MergeRowsWithAlias(a Row, aliasA string, b Row, aliasB string) Row {
	out := make(Row, len(a)+len(b)+4)
	for k, v := range a {
		out[k] = v
		if aliasA != "" {
			out[aliasA+"."+k] = v
		}
	}
	for k, v := range b {
		out[k] = v
		if aliasB != "" {
			out[aliasB+"."+k] = v
		}
	}
	return out
}

// CrossJoin returns the Cartesian product of left and right.
func CrossJoin(left, right []Row) []Row {
	if len(left) == 0 || len(right) == 0 {
		return nil
	}
	out := make([]Row, 0, len(left)*len(right))
	for _, l := range left {
		for _, r := range right {
			out = append(out, MergeRows(l, r))
		}
	}
	return out
}

// InnerJoin returns merged rows from left × right where pred(merged) is true.
func InnerJoin(left, right []Row, pred func(Row) bool) []Row {
	out := make([]Row, 0, len(left))
	for _, l := range left {
		for _, r := range right {
			merged := MergeRows(l, r)
			if pred == nil || pred(merged) {
				out = append(out, merged)
			}
		}
	}
	return out
}

// LeftOuterJoin returns all rows from left joined with matching rows from
// right.  When no right row matches, right columns are set to nil.
// rightCols lists the column names present in right rows.
func LeftOuterJoin(left, right []Row, pred func(Row) bool, rightCols []string) []Row {
	out := make([]Row, 0, len(left))
	for _, l := range left {
		matched := false
		for _, r := range right {
			merged := MergeRows(l, r)
			if pred == nil || pred(merged) {
				out = append(out, merged)
				matched = true
			}
		}
		if !matched {
			// Emit the left row with right columns set to nil.
			merged := make(Row, len(l)+len(rightCols))
			for k, v := range l {
				merged[k] = v
			}
			for _, col := range rightCols {
				merged[col] = nil
			}
			out = append(out, merged)
		}
	}
	return out
}
