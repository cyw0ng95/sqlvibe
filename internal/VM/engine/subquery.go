package engine

// ExistsRows returns true if the row slice is non-empty.
func ExistsRows(rows []Row) bool {
	return len(rows) > 0
}

// ScalarRow extracts a single scalar value from the first row of a subquery
// result.  colFn selects the desired column from that first row.  Returns nil
// if the result is empty.
func ScalarRow(rows []Row, colFn func(Row) interface{}) interface{} {
	if len(rows) == 0 {
		return nil
	}
	return colFn(rows[0])
}

// InRows checks whether value appears in the subquery result set.
// colFn extracts the comparison value from each result row.
// equal tests two values for equality.
func InRows(value interface{}, rows []Row, colFn func(Row) interface{}, equal func(a, b interface{}) bool) bool {
	if value == nil {
		return false
	}
	for _, r := range rows {
		if equal(value, colFn(r)) {
			return true
		}
	}
	return false
}

// NotInRows returns true when value does not appear in the subquery result set.
// NULL value always yields false (SQL three-valued-logic).
func NotInRows(value interface{}, rows []Row, colFn func(Row) interface{}, equal func(a, b interface{}) bool) bool {
	if value == nil {
		return false
	}
	for _, r := range rows {
		v := colFn(r)
		if v == nil {
			// If any right-side value is NULL, NOT IN is UNKNOWN → false.
			return false
		}
		if equal(value, v) {
			return false
		}
	}
	return true
}

// AllRows returns true when pred holds for every row.
// Returns true for an empty set (vacuous truth).
func AllRows(rows []Row, pred func(Row) bool) bool {
	for _, r := range rows {
		if !pred(r) {
			return false
		}
	}
	return true
}

// AnyRows returns true when pred holds for at least one row.
func AnyRows(rows []Row, pred func(Row) bool) bool {
	for _, r := range rows {
		if pred(r) {
			return true
		}
	}
	return false
}

// FilteredRows returns the subset of rows for which pred is true.
// This is an alias for FilterRows that lives in the subquery file for use in
// subquery result filtering.
func FilteredRows(rows []Row, pred func(Row) bool) []Row {
	return FilterRows(rows, pred)
}
