package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "columnar.h"
#include "value.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// ColumnStore stores data column-by-column for analytical workloads.
//
// This CGO version uses a dual-layer design:
//   - C++ columnar store (authoritative storage) — future SIMD/batch operations
//   - Go ColumnVectors (read cache) — O(1) vectorized access with no CGO overhead
//
// Writes update both layers. Reads use Go vectors for maximum performance.
// The "Direct C Pointer" pattern is used (no Go→C++→Go callbacks).
type ColumnStore struct {
	cStore   unsafe.Pointer // *C.svdb_column_store_t (authoritative C++ storage)
	vectors  []*ColumnVector
	colNames []string
	colTypes []ValueType
	colIdx   map[string]int
	deleted  []bool
	live     int
}

// NewColumnStore creates a ColumnStore with C++ authoritative storage and Go read cache.
func NewColumnStore(columns []string, types []ValueType) *ColumnStore {
	idx := make(map[string]int, len(columns))
	for i, col := range columns {
		idx[col] = i
	}

	cs := &ColumnStore{
		colNames: columns,
		colTypes: types,
		colIdx:   idx,
	}

	if len(columns) == 0 {
		return cs
	}

	// Build C arrays for column names and types.
	cNames := make([]*C.char, len(columns))
	cTypes := make([]C.int, len(columns))
	for i, name := range columns {
		cNames[i] = C.CString(name)
		cTypes[i] = C.int(int(types[i]))
	}
	defer func() {
		for _, p := range cNames {
			C.free(unsafe.Pointer(p))
		}
	}()

	cs.cStore = unsafe.Pointer(C.svdb_column_store_create(
		(**C.char)(unsafe.Pointer(&cNames[0])),
		(*C.int)(&cTypes[0]),
		C.int(len(columns)),
	))

	// Initialise Go read-cache vectors.
	vecs := make([]*ColumnVector, len(columns))
	for i, col := range columns {
		vecs[i] = NewColumnVector(col, types[i])
	}
	cs.vectors = vecs

	return cs
}

func (cs *ColumnStore) getCStore() *C.svdb_column_store_t {
	return (*C.svdb_column_store_t)(cs.cStore)
}

// AppendRow appends a row (one value per column), updating both C++ storage and Go cache.
func (cs *ColumnStore) AppendRow(vals []Value) {
	cs.deleted = append(cs.deleted, false)
	cs.live++

	// Update Go read-cache vectors (fast path for reads).
	for i, vec := range cs.vectors {
		if i < len(vals) {
			vec.Append(vals[i])
		} else {
			vec.AppendNull()
		}
	}

	// Update C++ authoritative store.
	if cs.cStore == nil {
		return
	}
	n := len(cs.colNames)
	cVals, cleanup := valsToC(vals, n)
	defer cleanup()
	C.svdb_column_store_append_row(cs.getCStore(), (*C.svdb_value_t)(&cVals[0]), C.int(n))
}

// GetRow reads a row across all columns using the Go cache (fast path).
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

// GetColumnByIdx returns the ColumnVector at position idx (O(1) from Go cache).
func (cs *ColumnStore) GetColumnByIdx(idx int) *ColumnVector {
	if idx < 0 || idx >= len(cs.vectors) {
		return nil
	}
	return cs.vectors[idx]
}

// DeleteRow marks a row as deleted in both C++ storage and Go cache.
func (cs *ColumnStore) DeleteRow(idx int) {
	if idx < 0 || idx >= len(cs.deleted) || cs.deleted[idx] {
		return
	}
	cs.deleted[idx] = true
	cs.live--
	if cs.cStore != nil {
		C.svdb_column_store_delete_row(cs.getCStore(), C.int(idx))
	}
}

// SetValue sets a specific cell value at (rowIdx, colIdx) in both C++ storage and Go cache.
func (cs *ColumnStore) SetValue(rowIdx, colIdx int, val Value) {
	if colIdx < 0 || colIdx >= len(cs.vectors) {
		return
	}
	vec := cs.vectors[colIdx]
	if rowIdx >= 0 && rowIdx < vec.Len() {
		vec.Set(rowIdx, val)
	}
	// Update C++ authoritative store.
	if cs.cStore == nil {
		return
	}
	n := len(cs.colNames)
	cVals := make([]C.svdb_value_t, n)
	var outCount C.int
	if C.svdb_column_store_get_row(cs.getCStore(), C.int(rowIdx), (*C.svdb_value_t)(&cVals[0]), &outCount) == 1 {
		if cs2 := goValueToC(val, &cVals[colIdx]); cs2 != nil {
			defer C.free(unsafe.Pointer(cs2))
		}
		C.svdb_column_store_update_row(cs.getCStore(), C.int(rowIdx), (*C.svdb_value_t)(&cVals[0]), C.int(int(outCount)))
	}
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

// ToRows converts live rows to Row format using the Go cache.
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

// Filter returns a RoaringBitmap of row indices where pred(value) is true using Go cache.
func (cs *ColumnStore) Filter(colName string, pred func(Value) bool) *RoaringBitmap {
	rb := NewRoaringBitmap()
	vec := cs.GetColumn(colName)
	if vec == nil {
		return rb
	}
	for i := 0; i < vec.Len(); i++ {
		if i >= len(cs.deleted) || cs.deleted[i] {
			continue
		}
		if pred(vec.Get(i)) {
			rb.Add(uint32(i))
		}
	}
	return rb
}

// Close frees C++ column store resources.
func (cs *ColumnStore) Close() {
	if cs.cStore != nil {
		C.svdb_column_store_destroy(cs.getCStore())
		cs.cStore = nil
	}
}

// goValueToC converts a Go Value to a C svdb_value_t.
// Returns the allocated *C.char (if any) for the caller to free after the C call.
func goValueToC(v Value, cv *C.svdb_value_t) *C.char {
	switch v.Type {
	case TypeNull:
		C.svdb_value_init_null(cv)
	case TypeInt:
		C.svdb_value_init_int(cv, C.int64_t(v.Int))
	case TypeFloat:
		C.svdb_value_init_float(cv, C.double(v.Float))
	case TypeBool:
		C.svdb_value_init_bool(cv, C.int(v.Int))
	case TypeString:
		cs := C.CString(v.Str) // C-allocated; caller must free after C call
		C.svdb_value_init_string(cv, cs, C.size_t(len(v.Str)))
		return cs
	case TypeBytes:
		if len(v.Bytes) > 0 {
			cs := C.CString(string(v.Bytes))
			C.svdb_value_init_bytes(cv, cs, C.size_t(len(v.Bytes)))
			return cs
		}
		C.svdb_value_init_bytes(cv, nil, 0)
	default:
		C.svdb_value_init_null(cv)
	}
	return nil
}

// valsToC converts a Go Value slice to a C svdb_value_t slice.
// Returns the cVals slice and a cleanup function that frees C-allocated strings.
func valsToC(vals []Value, n int) ([]C.svdb_value_t, func()) {
	cVals := make([]C.svdb_value_t, n)
	var allocs []*C.char
	for i := 0; i < n; i++ {
		v := Value{}
		if i < len(vals) {
			v = vals[i]
		}
		if cs := goValueToC(v, &cVals[i]); cs != nil {
			allocs = append(allocs, cs)
		}
	}
	return cVals, func() {
		for _, p := range allocs {
			C.free(unsafe.Pointer(p))
		}
	}
}
