package storage

// RowStore is a row-oriented in-memory store with tombstone-based deletion.
type RowStore struct {
	rows     []Row
	deleted  []bool
	columns  []string
	colTypes []ValueType
	colIdx   map[string]int
	live     int
}

// NewRowStore creates a RowStore with the given column definitions.
func NewRowStore(columns []string, types []ValueType) *RowStore {
	idx := make(map[string]int, len(columns))
	for i, c := range columns {
		idx[c] = i
	}
	return &RowStore{columns: columns, colTypes: types, colIdx: idx}
}

// Insert appends a row and returns its index.
func (rs *RowStore) Insert(row Row) int {
	rs.rows = append(rs.rows, row)
	rs.deleted = append(rs.deleted, false)
	rs.live++
	return len(rs.rows) - 1
}

// Get returns the row at idx (including deleted rows).
func (rs *RowStore) Get(idx int) Row {
	if idx < 0 || idx >= len(rs.rows) {
		return Row{}
	}
	return rs.rows[idx]
}

// Update replaces the row at idx.
func (rs *RowStore) Update(idx int, row Row) {
	if idx < 0 || idx >= len(rs.rows) {
		return
	}
	rs.rows[idx] = row
}

// Delete marks a row as deleted (tombstone).
func (rs *RowStore) Delete(idx int) {
	if idx < 0 || idx >= len(rs.rows) || rs.deleted[idx] {
		return
	}
	rs.deleted[idx] = true
	rs.live--
}

// Scan returns all non-deleted rows.
func (rs *RowStore) Scan() []Row {
	out := make([]Row, 0, rs.live)
	for i, row := range rs.rows {
		if !rs.deleted[i] {
			out = append(out, row)
		}
	}
	return out
}

// ScanIndices returns the indices of all non-deleted rows.
func (rs *RowStore) ScanIndices() []int {
	out := make([]int, 0, rs.live)
	for i := range rs.rows {
		if !rs.deleted[i] {
			out = append(out, i)
		}
	}
	return out
}

// RowCount returns the total number of rows including deleted ones.
func (rs *RowStore) RowCount() int { return len(rs.rows) }

// LiveCount returns the number of non-deleted rows.
func (rs *RowStore) LiveCount() int { return rs.live }

// Columns returns the column names.
func (rs *RowStore) Columns() []string { return rs.columns }

// ColumnTypes returns the column types.
func (rs *RowStore) ColumnTypes() []ValueType { return rs.colTypes }

// ColIndex returns the zero-based column index for name, or -1 if not found.
func (rs *RowStore) ColIndex(name string) int {
	if i, ok := rs.colIdx[name]; ok {
		return i
	}
	return -1
}

// ToColumnVectors converts the live rows into a slice of ColumnVectors.
func (rs *RowStore) ToColumnVectors() []*ColumnVector {
	vecs := make([]*ColumnVector, len(rs.columns))
	for i, col := range rs.columns {
		vecs[i] = NewColumnVector(col, rs.colTypes[i])
	}
	for ri, row := range rs.rows {
		if rs.deleted[ri] {
			continue
		}
		for ci := range rs.columns {
			vecs[ci].Append(row.Get(ci))
		}
	}
	return vecs
}
