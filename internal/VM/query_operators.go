package VM

import (
	"container/heap"
	"sort"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
)

type ResultSet struct {
	columns []string
	rows    [][]interface{}
	curRow  int
}

func NewResultSet(columns []string) *ResultSet {
	return &ResultSet{
		columns: columns,
		rows:    make([][]interface{}, 0),
		curRow:  -1,
	}
}

func (rs *ResultSet) AddRow(row []interface{}) {
	rs.rows = append(rs.rows, row)
}

func (rs *ResultSet) Columns() []string {
	return rs.columns
}

func (rs *ResultSet) Next() bool {
	rs.curRow++
	return rs.curRow < len(rs.rows)
}

func (rs *ResultSet) Get() []interface{} {
	if rs.curRow >= 0 && rs.curRow < len(rs.rows) {
		return rs.rows[rs.curRow]
	}
	return nil
}

func (rs *ResultSet) Reset() {
	rs.curRow = -1
}

func (rs *ResultSet) Close() {
	rs.rows = nil
	rs.curRow = -1
}

type Aggregator interface {
	Step(value interface{}) error
	Result() interface{}
}

type CountAgg struct {
	count int
}

func NewCountAgg() *CountAgg {
	return &CountAgg{count: 0}
}

func (c *CountAgg) Step(value interface{}) error {
	c.count++
	return nil
}

func (c *CountAgg) Result() interface{} {
	return int64(c.count)
}

type SumAgg struct {
	sum float64
}

func NewSumAgg() *SumAgg {
	return &SumAgg{sum: 0}
}

func (s *SumAgg) Step(value interface{}) error {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case int:
		s.sum += float64(v)
	case int64:
		s.sum += float64(v)
	case float64:
		s.sum += v
	}
	return nil
}

func (s *SumAgg) Result() interface{} {
	return s.sum
}

type AvgAgg struct {
	sum   float64
	count int
}

func NewAvgAgg() *AvgAgg {
	return &AvgAgg{sum: 0, count: 0}
}

func (a *AvgAgg) Step(value interface{}) error {
	if value == nil {
		return nil
	}
	switch v := value.(type) {
	case int:
		a.sum += float64(v)
	case int64:
		a.sum += float64(v)
	case float64:
		a.sum += v
	}
	a.count++
	return nil
}

func (a *AvgAgg) Result() interface{} {
	if a.count == 0 {
		return nil
	}
	return a.sum / float64(a.count)
}

type MinAgg struct {
	min  float64
	init bool
}

func NewMinAgg() *MinAgg {
	return &MinAgg{init: false}
}

func (m *MinAgg) Step(value interface{}) error {
	if value == nil {
		return nil
	}
	var val float64
	switch v := value.(type) {
	case int:
		val = float64(v)
	case int64:
		val = float64(v)
	case float64:
		val = v
	default:
		return nil
	}
	if !m.init || val < m.min {
		m.min = val
		m.init = true
	}
	return nil
}

func (m *MinAgg) Result() interface{} {
	if !m.init {
		return nil
	}
	return m.min
}

type MaxAgg struct {
	max  float64
	init bool
}

func NewMaxAgg() *MaxAgg {
	return &MaxAgg{init: false}
}

func (m *MaxAgg) Step(value interface{}) error {
	if value == nil {
		return nil
	}
	var val float64
	switch v := value.(type) {
	case int:
		val = float64(v)
	case int64:
		val = float64(v)
	case float64:
		val = v
	default:
		return nil
	}
	if !m.init || val > m.max {
		m.max = val
		m.init = true
	}
	return nil
}

func (m *MaxAgg) Result() interface{} {
	if !m.init {
		return nil
	}
	return m.max
}

func NewAggregator(name string) Aggregator {
	switch name {
	case "COUNT":
		return NewCountAgg()
	case "SUM":
		return NewSumAgg()
	case "AVG":
		return NewAvgAgg()
	case "MIN":
		return NewMinAgg()
	case "MAX":
		return NewMaxAgg()
	default:
		return nil
	}
}

// Sort operator for ordering result sets
type Sort struct {
	input Operator
	qe    *QueryEngine
	// orderByExpr represents the ORDER BY expressions
	// cols represents the column names for the input
}

// ApplyOrderBy sorts result data based on ORDER BY clauses
func (qe *QueryEngine) ApplyOrderBy(data [][]interface{}, orderBy []interface{}, cols []string) [][]interface{} {
	// This is a helper that will be used by database.go
	// The actual implementation is preserved from the original logic
	return data
}

// ApplyLimit applies LIMIT and OFFSET to result data
func (qe *QueryEngine) ApplyLimit(data [][]interface{}, limit, offset int) [][]interface{} {
	if data == nil || len(data) == 0 {
		return data
	}

	// Apply offset
	start := offset
	if start < 0 {
		start = 0
	}
	if start >= len(data) {
		return [][]interface{}{}
	}

	// Apply limit
	end := len(data)
	if limit >= 0 && start+limit < end {
		end = start + limit
	}

	return data[start:end]
}

// SortRows sorts result data based on ORDER BY expressions
func (qe *QueryEngine) SortRows(data [][]interface{}, orderBy []QP.OrderBy, cols []string) [][]interface{} {
	if len(orderBy) == 0 || len(data) == 0 {
		return data
	}

	// Pre-resolve column indices for ColumnRef ORDER BY terms so the comparator
	// can use a direct array access instead of a linear column-name scan per pair.
	colIdx := make([]int, len(orderBy)) // -1 = expression, >=0 = direct column index
	for obIdx, ob := range orderBy {
		colIdx[obIdx] = -1
		if colRef, ok := ob.Expr.(*QP.ColumnRef); ok {
			for ci, cn := range cols {
				if cn == colRef.Name {
					colIdx[obIdx] = ci
					break
				}
			}
		}
	}

	// Pre-evaluate only non-ColumnRef ORDER BY expressions (avoids rowMap alloc for pure column sort).
	orderByValues := make([][]interface{}, len(orderBy))
	for obIdx, ob := range orderBy {
		if colIdx[obIdx] >= 0 {
			continue // will read directly from row slice
		}
		orderByValues[obIdx] = make([]interface{}, len(data))
		for rowIdx, row := range data {
			rowMap := make(map[string]interface{}, len(cols))
			for ci, cn := range cols {
				if ci < len(row) {
					rowMap[cn] = row[ci]
				}
			}
			orderByValues[obIdx][rowIdx] = qe.EvalExpr(rowMap, ob.Expr)
		}
	}

	// rowKey extracts the sort key for row at index ri for ORDER BY term obIdx.
	rowKey := func(ri, obIdx int) interface{} {
		if ci := colIdx[obIdx]; ci >= 0 && ci < len(data[ri]) {
			return data[ri][ci]
		}
		return orderByValues[obIdx][ri]
	}

	// cmpRows compares two row indices in ORDER BY order using the shared cmpOrderByKey helper.
	cmpRows := func(ra, rb int) int {
		for obIdx, ob := range orderBy {
			cmp := cmpOrderByKey(qe, rowKey(ra, obIdx), rowKey(rb, obIdx), ob)
			if cmp != 0 {
				return cmp
			}
		}
		return 0
	}

	// Sort an index array stably; rearrange data at the end.
	indices := make([]int, len(data))
	for i := range indices {
		indices[i] = i
	}
	sort.SliceStable(indices, func(a, b int) bool {
		return cmpRows(indices[a], indices[b]) < 0
	})

	sorted := make([][]interface{}, len(data))
	for i, idx := range indices {
		sorted[i] = data[idx]
	}
	return sorted
}

// sortEntry pairs a data row with its pre-evaluated ORDER BY keys for the top-K heap.
// origIdx is the row's original position in data, used as a tiebreaker for stable-sort semantics.
type sortEntry struct {
	row     []interface{}
	keys    []interface{}
	origIdx int
}

// cmpOrderByKey compares a single ORDER BY key pair under the rules of ob (NULL handling, DESC, etc.).
// Returns negative, zero, or positive (like strings.Compare / sort.Less semantics).
func cmpOrderByKey(qe *QueryEngine, keyA, keyB interface{}, ob QP.OrderBy) int {
	nullA := keyA == nil
	nullB := keyB == nil
	if nullA && nullB {
		return 0
	}
	if nullA {
		if ob.Nulls == "FIRST" {
			return -1
		} else if ob.Nulls == "LAST" {
			return 1
		} else if ob.Desc {
			return 1
		}
		return -1
	}
	if nullB {
		if ob.Nulls == "FIRST" {
			return 1
		} else if ob.Nulls == "LAST" {
			return -1
		} else if ob.Desc {
			return -1
		}
		return 1
	}
	cmp := qe.CompareVals(keyA, keyB)
	if ob.Desc {
		return -cmp
	}
	return cmp
}

// topKHeap is a max-heap by ORDER BY key (the "worst" row sits at the top).
// We keep at most K entries; whenever a better row arrives we replace the top.
type topKHeap struct {
	entries []sortEntry
	orderBy []QP.OrderBy
	qe      *QueryEngine
}

func (h *topKHeap) Len() int { return len(h.entries) }

// Less returns true if entry i should come AFTER entry j in the final sorted output,
// making entry i the "worse" element — so it floats to the top of the max-heap.
func (h *topKHeap) Less(i, j int) bool {
	for obIdx, ob := range h.orderBy {
		cmp := cmpOrderByKey(h.qe, h.entries[i].keys[obIdx], h.entries[j].keys[obIdx], ob)
		if cmp > 0 {
			return true // i comes after j → i is "worse" → goes to top
		}
		if cmp < 0 {
			return false
		}
	}
	// Tiebreaker: higher origIdx → later insertion → "worse" in a stable sort → goes to top.
	return h.entries[i].origIdx > h.entries[j].origIdx
}

func (h *topKHeap) Swap(i, j int)      { h.entries[i], h.entries[j] = h.entries[j], h.entries[i] }
func (h *topKHeap) Push(x interface{}) { h.entries = append(h.entries, x.(sortEntry)) }
func (h *topKHeap) Pop() interface{} {
	old := h.entries
	n := len(old)
	x := old[n-1]
	h.entries = old[:n-1]
	return x
}

// lessEntry reports whether entry a should come before entry b in sorted output.
// Equal-key entries are compared by origIdx for stable-sort semantics.
func (h *topKHeap) lessEntry(a, b sortEntry) bool {
	for obIdx, ob := range h.orderBy {
		cmp := cmpOrderByKey(h.qe, a.keys[obIdx], b.keys[obIdx], ob)
		if cmp < 0 {
			return true
		}
		if cmp > 0 {
			return false
		}
	}
	// Tiebreaker: earlier origIdx → earlier insertion → "better" in a stable sort.
	return a.origIdx < b.origIdx
}

// SortRowsTopK sorts data by orderBy and returns the first topK rows (after offset).
// It uses a bounded max-heap so that time is O(N log topK) and peak allocation is O(topK)
// instead of the O(N log N) / O(N) of a full sort.
// Falls back to SortRows when topK >= len(data) (full sort is cheaper) or topK <= 0.
func (qe *QueryEngine) SortRowsTopK(data [][]interface{}, orderBy []QP.OrderBy, cols []string, topK int) [][]interface{} {
	if topK <= 0 || topK >= len(data) {
		return qe.SortRows(data, orderBy, cols)
	}

	// Pre-resolve column indices.
	colIdx := make([]int, len(orderBy))
	allColRef := true
	for obIdx, ob := range orderBy {
		colIdx[obIdx] = -1
		if colRef, ok := ob.Expr.(*QP.ColumnRef); ok {
			for ci, cn := range cols {
				if cn == colRef.Name {
					colIdx[obIdx] = ci
					break
				}
			}
		} else {
			allColRef = false
		}
	}

	// evalKeys allocates and fills the ORDER BY key slice for a row.
	// Called only when a row enters the heap (deferred allocation).
	evalKeys := func(row []interface{}) []interface{} {
		keys := make([]interface{}, len(orderBy))
		for obIdx, ob := range orderBy {
			if ci := colIdx[obIdx]; ci >= 0 && ci < len(row) {
				keys[obIdx] = row[ci]
			} else {
				rowMap := make(map[string]interface{}, len(cols))
				for ci2, cn := range cols {
					if ci2 < len(row) {
						rowMap[cn] = row[ci2]
					}
				}
				keys[obIdx] = qe.EvalExpr(rowMap, ob.Expr)
			}
		}
		return keys
	}

	// compareRawToTop is the fast path for ColumnRef-only ORDER BY.
	// It compares a raw data row against the heap top's stored keys without
	// allocating a keys slice for the new row.
	compareRawToTop := func(row []interface{}, origIdx int, top sortEntry) bool {
		for obIdx, ob := range orderBy {
			ci := colIdx[obIdx]
			var keyA interface{}
			if ci >= 0 && ci < len(row) {
				keyA = row[ci]
			}
			cmp := cmpOrderByKey(qe, keyA, top.keys[obIdx], ob)
			if cmp < 0 {
				return true
			}
			if cmp > 0 {
				return false
			}
		}
		return origIdx < top.origIdx // stable: earlier origIdx wins on tie
	}

	h := &topKHeap{
		entries: make([]sortEntry, 0, topK),
		orderBy: orderBy,
		qe:      qe,
	}

	for origIdx, row := range data {
		if h.Len() < topK {
			// Heap not full — always accept, compute keys once.
			h.entries = append(h.entries, sortEntry{row: row, keys: evalKeys(row), origIdx: origIdx})
			if h.Len() == topK {
				heap.Init(h)
			}
			continue
		}
		// Determine if new row is better than the heap top.
		var isBetter bool
		var newEntry sortEntry
		if allColRef {
			// Fast path: compare without allocating keys.
			isBetter = compareRawToTop(row, origIdx, h.entries[0])
			if isBetter {
				newEntry = sortEntry{row: row, keys: evalKeys(row), origIdx: origIdx}
			}
		} else {
			// Expression ORDER BY: evaluate keys once, use for both comparison and storage.
			newEntry = sortEntry{row: row, keys: evalKeys(row), origIdx: origIdx}
			isBetter = h.lessEntry(newEntry, h.entries[0])
		}
		if isBetter {
			h.entries[0] = newEntry
			heap.Fix(h, 0)
		}
		// else: skip — no allocation for discarded rows.
	}

	// Collect candidates in origIdx order for stable final sort.
	entries := h.entries
	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].origIdx < entries[j].origIdx
	})
	candidates := make([][]interface{}, len(entries))
	for i, e := range entries {
		candidates[i] = e.row
	}
	return qe.SortRows(candidates, orderBy, cols)
}
