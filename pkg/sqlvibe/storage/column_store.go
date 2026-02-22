package storage

// ColumnStore stores data column-by-column for analytical workloads.
type ColumnStore struct {
	vectors  []*ColumnVector
	colNames []string
	colIdx   map[string]int
	deleted  []bool
	live     int
}

// NewColumnStore creates a ColumnStore with the given column definitions.
func NewColumnStore(columns []string, types []ValueType) *ColumnStore {
	vecs := make([]*ColumnVector, len(columns))
	idx := make(map[string]int, len(columns))
	for i, col := range columns {
		vecs[i] = NewColumnVector(col, types[i])
		idx[col] = i
	}
	return &ColumnStore{vectors: vecs, colNames: columns, colIdx: idx}
}

// AppendRow appends a row (one value per column).
func (cs *ColumnStore) AppendRow(vals []Value) {
	cs.deleted = append(cs.deleted, false)
	cs.live++
	for i, vec := range cs.vectors {
		if i < len(vals) {
			vec.Append(vals[i])
		} else {
			vec.AppendNull()
		}
	}
}

// GetRow reads a row across all columns.
func (cs *ColumnStore) GetRow(idx int) []Value {
	if idx < 0 || idx >= len(cs.deleted) {
		return nil
	}
	out := make([]Value, len(cs.vectors))
	for i, vec := range cs.vectors {
		out[i] = vec.Get(idx)
	}
	return out
}

// GetColumn returns the ColumnVector for the named column (nil if not found).
func (cs *ColumnStore) GetColumn(name string) *ColumnVector {
	i, ok := cs.colIdx[name]
	if !ok {
		return nil
	}
	return cs.vectors[i]
}

// GetColumnByIdx returns the ColumnVector at position idx.
func (cs *ColumnStore) GetColumnByIdx(idx int) *ColumnVector {
	if idx < 0 || idx >= len(cs.vectors) {
		return nil
	}
	return cs.vectors[idx]
}

// DeleteRow marks a row as deleted.
func (cs *ColumnStore) DeleteRow(idx int) {
	if idx < 0 || idx >= len(cs.deleted) || cs.deleted[idx] {
		return
	}
	cs.deleted[idx] = true
	cs.live--
}

// RowCount returns the total number of rows including deleted.
func (cs *ColumnStore) RowCount() int { return len(cs.deleted) }

// LiveCount returns the number of non-deleted rows.
func (cs *ColumnStore) LiveCount() int { return cs.live }

// Columns returns the column names.
func (cs *ColumnStore) Columns() []string { return cs.colNames }

// ColIndex returns the zero-based index for a column name (-1 if not found).
func (cs *ColumnStore) ColIndex(name string) int {
	if i, ok := cs.colIdx[name]; ok {
		return i
	}
	return -1
}

// ToRows converts live rows to Row format.
func (cs *ColumnStore) ToRows() []Row {
	out := make([]Row, 0, cs.live)
	for i := range cs.deleted {
		if cs.deleted[i] {
			continue
		}
		vals := cs.GetRow(i)
		out = append(out, NewRow(vals))
	}
	return out
}

// Filter returns a RoaringBitmap of row indices where pred(value) is true. Deleted rows are excluded.
func (cs *ColumnStore) Filter(colName string, pred func(Value) bool) *RoaringBitmap {
	rb := NewRoaringBitmap()
	vec := cs.GetColumn(colName)
	if vec == nil {
		return rb
	}
	for i := 0; i < vec.Len(); i++ {
		if cs.deleted[i] {
			continue
		}
		if pred(vec.Get(i)) {
			rb.Add(uint32(i))
		}
	}
	return rb
}
