// Package engine provides standalone SQL query engine utilities.
// Functions in this package operate on Row data without depending on the full
// QueryEngine struct, making them easy to compose and test independently.
package engine

// Row is a single database row represented as a column-name → value map.
type Row = map[string]interface{}

// FilterRows returns only those rows for which pred returns true.
// A nil predicate returns all rows unchanged.
func FilterRows(rows []Row, pred func(Row) bool) []Row {
	if pred == nil {
		return rows
	}
	out := make([]Row, 0, len(rows))
	for _, r := range rows {
		if pred(r) {
			out = append(out, r)
		}
	}
	return out
}

// ProjectRow applies a set of projection functions to a single row and returns
// the resulting row. projections maps output column name → function that
// computes the value from the input row.
func ProjectRow(row Row, projections map[string]func(Row) interface{}) Row {
	out := make(Row, len(projections))
	for col, fn := range projections {
		out[col] = fn(row)
	}
	return out
}

// ProjectRows applies ProjectRow to every row in rows.
func ProjectRows(rows []Row, projections map[string]func(Row) interface{}) []Row {
	out := make([]Row, len(rows))
	for i, r := range rows {
		out[i] = ProjectRow(r, projections)
	}
	return out
}

// ApplyDistinct removes duplicate rows using keyFn to compute a deduplication
// key for each row. The first occurrence of each key is retained.
func ApplyDistinct(rows []Row, keyFn func(Row) string) []Row {
	seen := make(map[string]struct{}, len(rows))
	out := make([]Row, 0, len(rows))
	for _, r := range rows {
		k := keyFn(r)
		if _, exists := seen[k]; !exists {
			seen[k] = struct{}{}
			out = append(out, r)
		}
	}
	return out
}

// ApplyLimitOffset returns a sub-slice of rows after skipping offset rows and
// returning at most limit rows.  A limit of ≤ 0 means no upper bound.
func ApplyLimitOffset(rows []Row, limit, offset int) []Row {
	if offset < 0 {
		offset = 0
	}
	if offset >= len(rows) {
		return nil
	}
	rows = rows[offset:]
	if limit > 0 && limit < len(rows) {
		rows = rows[:limit]
	}
	return rows
}

// ColNames returns a deduplicated, ordered list of all column names present in
// any row of the result set.
func ColNames(rows []Row) []string {
	seen := make(map[string]struct{})
	var out []string
	for _, r := range rows {
		for k := range r {
			if _, ok := seen[k]; !ok {
				seen[k] = struct{}{}
				out = append(out, k)
			}
		}
	}
	return out
}
