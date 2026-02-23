package DS

// ColumnVector stores a single column's values in typed backing slices for cache-friendly access.
type ColumnVector struct {
	Name    string
	Type    ValueType
	ints    []int64
	floats  []float64
	strings []string
	bytes   [][]byte
	nulls   []bool
}

// NewColumnVector creates an empty ColumnVector with the given name and type.
func NewColumnVector(name string, typ ValueType) *ColumnVector {
	return &ColumnVector{Name: name, Type: typ}
}

// Len returns the number of elements (including nulls).
func (cv *ColumnVector) Len() int { return len(cv.nulls) }

// IsNull returns true if element i is NULL.
func (cv *ColumnVector) IsNull(i int) bool {
	if i < 0 || i >= len(cv.nulls) {
		return false
	}
	return cv.nulls[i]
}

// SetNull updates the null flag for element i.
func (cv *ColumnVector) SetNull(i int, null bool) {
	if i >= 0 && i < len(cv.nulls) {
		cv.nulls[i] = null
	}
}

// Set overwrites the value at index i.
func (cv *ColumnVector) Set(i int, v Value) {
	if i < 0 || i >= len(cv.nulls) {
		return
	}
	if v.IsNull() {
		cv.nulls[i] = true
		return
	}
	cv.nulls[i] = false
	switch cv.Type {
	case TypeInt, TypeBool:
		if i < len(cv.ints) {
			cv.ints[i] = v.Int
		}
	case TypeFloat:
		if i < len(cv.floats) {
			cv.floats[i] = v.Float
		}
	case TypeString:
		if i < len(cv.strings) {
			cv.strings[i] = v.Str
		}
	case TypeBytes:
		if i < len(cv.bytes) {
			cv.bytes[i] = v.Bytes
		}
	}
}

// Append appends a Value to the vector.
func (cv *ColumnVector) Append(v Value) {
	if v.IsNull() {
		cv.AppendNull()
		return
	}
	cv.nulls = append(cv.nulls, false)
	switch cv.Type {
	case TypeInt, TypeBool:
		cv.ints = append(cv.ints, v.Int)
	case TypeFloat:
		cv.floats = append(cv.floats, v.Float)
	case TypeString:
		cv.strings = append(cv.strings, v.Str)
	case TypeBytes:
		cv.bytes = append(cv.bytes, v.Bytes)
	default:
		cv.ints = append(cv.ints, 0)
	}
}

// AppendNull appends a NULL element.
func (cv *ColumnVector) AppendNull() {
	cv.nulls = append(cv.nulls, true)
	switch cv.Type {
	case TypeInt, TypeBool:
		cv.ints = append(cv.ints, 0)
	case TypeFloat:
		cv.floats = append(cv.floats, 0)
	case TypeString:
		cv.strings = append(cv.strings, "")
	case TypeBytes:
		cv.bytes = append(cv.bytes, nil)
	default:
		cv.ints = append(cv.ints, 0)
	}
}

// Get returns the Value at index i.
func (cv *ColumnVector) Get(i int) Value {
	if i < 0 || i >= len(cv.nulls) {
		return NullValue()
	}
	if cv.nulls[i] {
		return NullValue()
	}
	switch cv.Type {
	case TypeInt:
		return IntValue(cv.ints[i])
	case TypeFloat:
		return FloatValue(cv.floats[i])
	case TypeString:
		return StringValue(cv.strings[i])
	case TypeBytes:
		return BytesValue(cv.bytes[i])
	case TypeBool:
		return BoolValue(cv.ints[i] != 0)
	}
	return NullValue()
}

// Reset clears all data.
func (cv *ColumnVector) Reset() {
	cv.ints = cv.ints[:0]
	cv.floats = cv.floats[:0]
	cv.strings = cv.strings[:0]
	cv.bytes = cv.bytes[:0]
	cv.nulls = cv.nulls[:0]
}

// Slice returns a new ColumnVector containing elements [start, end).
func (cv *ColumnVector) Slice(start, end int) *ColumnVector {
	if start < 0 {
		start = 0
	}
	if end > cv.Len() {
		end = cv.Len()
	}
	out := NewColumnVector(cv.Name, cv.Type)
	for i := start; i < end; i++ {
		out.Append(cv.Get(i))
	}
	return out
}

// Project returns a new ColumnVector containing only the values at the given indices.
func (cv *ColumnVector) Project(indices []int) *ColumnVector {
result := NewColumnVector(cv.Name, cv.Type)
result.nulls = make([]bool, len(indices))
switch cv.Type {
case TypeInt, TypeBool:
result.ints = make([]int64, len(indices))
for i, idx := range indices {
result.nulls[i] = cv.nulls[idx]
result.ints[i] = cv.ints[idx]
}
case TypeFloat:
result.floats = make([]float64, len(indices))
for i, idx := range indices {
result.nulls[i] = cv.nulls[idx]
result.floats[i] = cv.floats[idx]
}
case TypeString:
result.strings = make([]string, len(indices))
for i, idx := range indices {
result.nulls[i] = cv.nulls[idx]
result.strings[i] = cv.strings[idx]
}
case TypeBytes:
result.bytes = make([][]byte, len(indices))
for i, idx := range indices {
result.nulls[i] = cv.nulls[idx]
result.bytes[i] = cv.bytes[idx]
}
}
return result
}

// Ints returns the underlying int64 slice.
func (cv *ColumnVector) Ints() []int64 { return cv.ints }

// Floats returns the underlying float64 slice.
func (cv *ColumnVector) Floats() []float64 { return cv.floats }

// Strings returns the underlying string slice.
func (cv *ColumnVector) Strings() []string { return cv.strings }
