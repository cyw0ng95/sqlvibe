package DS

// Mode represents the recommended storage mode for a workload.
type Mode int

const (
	ModeRow      Mode = iota // optimised for writes / point lookups
	ModeColumnar             // optimised for filter-heavy analytical scans
	ModeHybrid               // balanced mixed workload
)

// QueryStats tracks query-pattern counters used by RecommendMode.
type QueryStats struct {
	FilterQueries int64 // equality / range filter queries
	ScanQueries   int64 // full-table scan queries
	WriteQueries  int64 // insert / update / delete operations
}

// HybridStore combines RowStore, ColumnStore, IndexEngine and Arena into a single
// unified storage interface. Writes go to both row and column stores; reads can
// leverage indexes for fast path or fall back to linear scan.
type HybridStore struct {
	rowStore    *RowStore
	colStore    *ColumnStore
	indexEngine *IndexEngine
	arena       *Arena
	columns     []string
	colTypes    []ValueType
	stats       QueryStats
}

// NewHybridStore creates a HybridStore with the given column definitions.
func NewHybridStore(columns []string, types []ValueType) *HybridStore {
	return &HybridStore{
		rowStore:    NewRowStore(columns, types),
		colStore:    NewColumnStore(columns, types),
		indexEngine: NewIndexEngine(),
		arena:       NewArena(64 * 1024),
		columns:     columns,
		colTypes:    types,
	}
}

// Insert appends a row and returns its row index.
func (hs *HybridStore) Insert(vals []Value) int {
	row := NewRow(vals)
	idx := hs.rowStore.Insert(row)
	hs.colStore.AppendRow(vals)

	// Update indexes
	for ci, col := range hs.columns {
		val := NullValue()
		if ci < len(vals) {
			val = vals[ci]
		}
		if hs.indexEngine.HasBitmapIndex(col) || hs.indexEngine.HasSkipListIndex(col) {
			hs.indexEngine.IndexRow(uint32(idx), col, val)
		}
	}
	return idx
}

// Update replaces the values at rowIdx.
func (hs *HybridStore) Update(rowIdx int, vals []Value) {
	// Un-index old values
	oldRow := hs.rowStore.Get(rowIdx)
	for ci, col := range hs.columns {
		if hs.indexEngine.HasBitmapIndex(col) || hs.indexEngine.HasSkipListIndex(col) {
			hs.indexEngine.UnindexRow(uint32(rowIdx), col, oldRow.Get(ci))
		}
	}

	newRow := NewRow(vals)
	hs.rowStore.Update(rowIdx, newRow)

	// Update column store by overwriting each column vector value
	for ci, vec := range hs.colStore.vectors {
		if ci < len(vals) {
			if rowIdx >= 0 && rowIdx < vec.Len() {
				vec.Set(rowIdx, vals[ci])
			}
		}
	}

	// Re-index new values
	for ci, col := range hs.columns {
		if hs.indexEngine.HasBitmapIndex(col) || hs.indexEngine.HasSkipListIndex(col) {
			val := NullValue()
			if ci < len(vals) {
				val = vals[ci]
			}
			hs.indexEngine.IndexRow(uint32(rowIdx), col, val)
		}
	}
}

// Delete removes a row from the store and all indexes.
func (hs *HybridStore) Delete(rowIdx int) {
	row := hs.rowStore.Get(rowIdx)
	for ci, col := range hs.columns {
		if hs.indexEngine.HasBitmapIndex(col) || hs.indexEngine.HasSkipListIndex(col) {
			hs.indexEngine.UnindexRow(uint32(rowIdx), col, row.Get(ci))
		}
	}
	hs.rowStore.Delete(rowIdx)
	hs.colStore.DeleteRow(rowIdx)
}

// Scan returns all live rows as []Value slices.
func (hs *HybridStore) Scan() [][]Value {
	indices := hs.rowStore.ScanIndices()
	out := make([][]Value, 0, len(indices))
	for _, i := range indices {
		row := hs.rowStore.Get(i)
		vals := make([]Value, len(hs.columns))
		for ci := range hs.columns {
			vals[ci] = row.Get(ci)
		}
		out = append(out, vals)
	}
	return out
}

// ScanWhere returns rows where colName == val, using an index when available.
func (hs *HybridStore) ScanWhere(colName string, val Value) [][]Value {
	colIdx := hs.rowStore.ColIndex(colName)
	if colIdx < 0 {
		return nil
	}

	// Use index if available
	if hs.indexEngine.HasBitmapIndex(colName) || hs.indexEngine.HasSkipListIndex(colName) {
		rb := hs.indexEngine.LookupEqual(colName, val)
		if rb != nil {
			return hs.collectRows(rb.ToSlice())
		}
	}

	// Linear scan fallback
	var out [][]Value
	for _, i := range hs.rowStore.ScanIndices() {
		row := hs.rowStore.Get(i)
		if row.Get(colIdx).Equal(val) {
			vals := make([]Value, len(hs.columns))
			for ci := range hs.columns {
				vals[ci] = row.Get(ci)
			}
			out = append(out, vals)
		}
	}
	return out
}

// ScanRange returns rows where colName is in [lo, hi], using a skip-list index when available.
func (hs *HybridStore) ScanRange(colName string, lo, hi Value) [][]Value {
	colIdx := hs.rowStore.ColIndex(colName)
	if colIdx < 0 {
		return nil
	}

	if hs.indexEngine.HasSkipListIndex(colName) {
		rb := hs.indexEngine.LookupRange(colName, lo, hi, true)
		if rb != nil {
			return hs.collectRows(rb.ToSlice())
		}
	}

	// Linear scan fallback
	var out [][]Value
	for _, i := range hs.rowStore.ScanIndices() {
		row := hs.rowStore.Get(i)
		v := row.Get(colIdx)
		if Compare(v, lo) >= 0 && Compare(v, hi) <= 0 {
			vals := make([]Value, len(hs.columns))
			for ci := range hs.columns {
				vals[ci] = row.Get(ci)
			}
			out = append(out, vals)
		}
	}
	return out
}

// CreateIndex creates an index on the named column.
func (hs *HybridStore) CreateIndex(colName string, useSkipList bool) {
	if useSkipList {
		hs.indexEngine.AddSkipListIndex(colName)
	} else {
		hs.indexEngine.AddBitmapIndex(colName)
	}
	// Back-fill existing rows
	colIdx := hs.rowStore.ColIndex(colName)
	if colIdx < 0 {
		return
	}
	for _, i := range hs.rowStore.ScanIndices() {
		row := hs.rowStore.Get(i)
		hs.indexEngine.IndexRow(uint32(i), colName, row.Get(colIdx))
	}
}

// RowCount returns the total number of rows including deleted.
func (hs *HybridStore) RowCount() int { return hs.rowStore.RowCount() }

// LiveCount returns the number of non-deleted rows.
func (hs *HybridStore) LiveCount() int { return hs.rowStore.LiveCount() }

// Columns returns the column names.
func (hs *HybridStore) Columns() []string { return hs.columns }

// ColType returns the ValueType for the named column, or TypeNull if not found.
func (hs *HybridStore) ColType(name string) ValueType {
	for i, col := range hs.columns {
		if col == name {
			return hs.colTypes[i]
		}
	}
	return TypeNull
}

// ColIndex returns the zero-based index for a column name (-1 if not found).
func (hs *HybridStore) ColIndex(name string) int { return hs.rowStore.ColIndex(name) }

func (hs *HybridStore) collectRows(idxs []uint32) [][]Value {
	out := make([][]Value, 0, len(idxs))
	for _, ui := range idxs {
		i := int(ui)
		row := hs.rowStore.Get(i)
		vals := make([]Value, len(hs.columns))
		for ci := range hs.columns {
			vals[ci] = row.Get(ci)
		}
		out = append(out, vals)
	}
	return out
}

// ColStore returns the underlying ColumnStore (for vectorized operations).
func (hs *HybridStore) ColStore() *ColumnStore { return hs.colStore }

// RecordFilter increments the filter-query counter.
func (hs *HybridStore) RecordFilter() { hs.stats.FilterQueries++ }

// RecordScan increments the full-scan counter.
func (hs *HybridStore) RecordScan() { hs.stats.ScanQueries++ }

// RecordWrite increments the write counter.
func (hs *HybridStore) RecordWrite() { hs.stats.WriteQueries++ }

// Stats returns a copy of the current QueryStats.
func (hs *HybridStore) Stats() QueryStats { return hs.stats }

// RecommendMode returns the storage mode best suited to the observed workload.
//
// Heuristics use division to avoid integer overflow:
//   - FilterQueries/max(ScanQueries,1) >= 2  → ModeColumnar (filter-heavy analytical)
//   - WriteQueries/max(FilterQueries,1) >= 3 → ModeRow      (write-heavy transactional)
//   - otherwise                               → ModeHybrid
func (hs *HybridStore) RecommendMode() Mode {
	s := hs.stats
	scans := s.ScanQueries
	if scans < 1 {
		scans = 1
	}
	filters := s.FilterQueries
	if filters < 1 {
		filters = 1
	}
	if s.FilterQueries/scans >= 2 {
		return ModeColumnar
	}
	if s.WriteQueries/filters >= 3 {
		return ModeRow
	}
	return ModeHybrid
}

// compareOp compares two Values using a relational operator string.
// op can be "=", "!=", "<", "<=", ">", ">=".
func compareOp(a, b Value, op string) bool {
	cmp := Compare(a, b)
	switch op {
	case "=":
		return cmp == 0
	case "!=":
		return cmp != 0
	case "<":
		return cmp < 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case ">=":
		return cmp >= 0
	}
	return false
}

// MaxValue returns a sentinel Value used as a range upper bound sentinel in ScanRange.
// Results are always post-filtered with compareOp to ensure strict/inclusive semantics.
func MaxValue() Value {
	return Value{Type: TypeString, Str: "\U0010FFFF"}
}

// MinValue returns a sentinel Value that compares less than all real values.
func MinValue() Value {
	return Value{Type: TypeNull}
}

// ScanWithFilter returns rows matching the given predicate using vectorized evaluation.
// op can be "=", "!=", "<", "<=", ">", ">=".
func (hs *HybridStore) ScanWithFilter(colName string, op string, val Value) [][]Value {
	colIdx := hs.rowStore.ColIndex(colName)
	if colIdx < 0 {
		return nil
	}
	// For equality, try index first
	if op == "=" {
		return hs.ScanWhere(colName, val)
	}
	// For range ops, try skip list index
	if (op == "<" || op == "<=" || op == ">" || op == ">=") && hs.indexEngine.HasSkipListIndex(colName) {
		var lo, hi Value
		switch op {
		case ">", ">=":
			lo = val
			hi = MaxValue()
		case "<", "<=":
			lo = MinValue()
			hi = val
		}
		candidates := hs.ScanRange(colName, lo, hi)
		var out [][]Value
		for _, row := range candidates {
			v := row[colIdx]
			if compareOp(v, val, op) {
				out = append(out, row)
			}
		}
		return out
	}
	// Linear scan fallback
	var out [][]Value
	for _, i := range hs.rowStore.ScanIndices() {
		row := hs.rowStore.Get(i)
		v := row.Get(colIdx)
		if compareOp(v, val, op) {
			vals := make([]Value, len(hs.columns))
			for ci := range hs.columns {
				vals[ci] = row.Get(ci)
			}
			out = append(out, vals)
		}
	}
	return out
}

// ScanProjected returns all rows with only the requested columns materialized.
func (hs *HybridStore) ScanProjected(requiredCols []string) [][]Value {
	colIndices := make([]int, len(requiredCols))
	for i, col := range requiredCols {
		colIndices[i] = hs.ColIndex(col)
	}
	indices := hs.rowStore.ScanIndices()
	out := make([][]Value, 0, len(indices))
	for _, rowIdx := range indices {
		row := hs.rowStore.Get(rowIdx)
		vals := make([]Value, len(requiredCols))
		for i, colIdx := range colIndices {
			if colIdx >= 0 {
				vals[i] = row.Get(colIdx)
			} else {
				vals[i] = NullValue()
			}
		}
		out = append(out, vals)
	}
	return out
}

// ScanProjectedWhere returns filtered rows with only the requested columns.
func (hs *HybridStore) ScanProjectedWhere(colName string, val Value, requiredCols []string) [][]Value {
	colIndices := make([]int, len(requiredCols))
	for i, col := range requiredCols {
		colIndices[i] = hs.ColIndex(col)
	}
	filterIdx := hs.ColIndex(colName)

	projectRows := func(rowIDs []uint32) [][]Value {
		out := make([][]Value, 0, len(rowIDs))
		for _, rid := range rowIDs {
			row := hs.rowStore.Get(int(rid))
			vals := make([]Value, len(requiredCols))
			for i, ci := range colIndices {
				if ci >= 0 {
					vals[i] = row.Get(ci)
				} else {
					vals[i] = NullValue()
				}
			}
			out = append(out, vals)
		}
		return out
	}

	if hs.indexEngine.HasBitmapIndex(colName) {
		rb := hs.indexEngine.LookupEqual(colName, val)
		if rb != nil {
			return projectRows(rb.ToSlice())
		}
	}

	var rowIDs []uint32
	for _, i := range hs.rowStore.ScanIndices() {
		row := hs.rowStore.Get(i)
		if filterIdx >= 0 && row.Get(filterIdx).Equal(val) {
			rowIDs = append(rowIDs, uint32(i))
		}
	}
	return projectRows(rowIDs)
}
