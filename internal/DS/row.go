package DS

// Row holds a single database row. Bitmap tracks which columns are NULL (bit i set → col i is NULL).
type Row struct {
	Cols   []Value
	Bitmap uint64
}

// NewRow creates a Row from a slice of Values. Columns whose Value is TypeNull have their bitmap bit set.
func NewRow(cols []Value) Row {
	r := Row{Cols: make([]Value, len(cols))}
	for i, v := range cols {
		r.Cols[i] = v
		if v.IsNull() {
			r.SetNull(i)
		}
	}
	return r
}

// IsNull returns true if column idx is NULL.
func (r *Row) IsNull(idx int) bool {
	if idx < 0 || idx >= 64 {
		return false
	}
	return (r.Bitmap>>uint(idx))&1 == 1
}

// SetNull marks column idx as NULL.
func (r *Row) SetNull(idx int) {
	if idx >= 0 && idx < 64 {
		r.Bitmap |= 1 << uint(idx)
	}
}

// ClearNull clears the NULL mark for column idx.
func (r *Row) ClearNull(idx int) {
	if idx >= 0 && idx < 64 {
		r.Bitmap &^= 1 << uint(idx)
	}
}

// Get returns the Value at column idx. If the column is marked NULL, NullValue() is returned.
func (r *Row) Get(idx int) Value {
	if idx < 0 || idx >= len(r.Cols) {
		return NullValue()
	}
	if r.IsNull(idx) {
		return NullValue()
	}
	return r.Cols[idx]
}

// Set stores v at column idx and updates the null bitmap accordingly.
func (r *Row) Set(idx int, v Value) {
	if idx < 0 || idx >= len(r.Cols) {
		return
	}
	r.Cols[idx] = v
	if v.IsNull() {
		r.SetNull(idx)
	} else {
		r.ClearNull(idx)
	}
}

// Len returns the number of columns.
func (r *Row) Len() int { return len(r.Cols) }
