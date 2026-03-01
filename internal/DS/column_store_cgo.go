//go:build SVDB_ENABLE_CGO_DS

package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "columnar.h"
#include "value.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import "unsafe"

// ColumnStore stores data column-by-column for analytical workloads.
// This CGO version delegates storage to the C++ columnar engine (Direct C Pointer pattern).
type ColumnStore struct {
	cStore   unsafe.Pointer // *C.svdb_column_store_t
	colNames []string
	colTypes []ValueType
	colIdx   map[string]int
}

// NewColumnStore creates a ColumnStore backed by C++ columnar storage.
func NewColumnStore(columns []string, types []ValueType) *ColumnStore {
	idx := make(map[string]int, len(columns))
	for i, col := range columns {
		idx[col] = i
	}
	if len(columns) == 0 {
		return &ColumnStore{colNames: columns, colTypes: types, colIdx: idx}
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

	cStore := C.svdb_column_store_create(
		(**C.char)(unsafe.Pointer(&cNames[0])),
		(*C.int)(&cTypes[0]),
		C.int(len(columns)),
	)

	return &ColumnStore{
		cStore:   unsafe.Pointer(cStore),
		colNames: columns,
		colTypes: types,
		colIdx:   idx,
	}
}

func (cs *ColumnStore) getCStore() *C.svdb_column_store_t {
	return (*C.svdb_column_store_t)(cs.cStore)
}

// goValueToC converts a Go Value to a C svdb_value_t.
// For strings, cs must be a non-nil *C.char returned by C.CString - caller is
// responsible for freeing after the C call.  Pass nil cstr for non-string types.
// Returns the allocated *C.char (if any) for the caller to free.
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

// AppendRow appends a row (one value per column).
func (cs *ColumnStore) AppendRow(vals []Value) {
	if cs.cStore == nil {
		return
	}
	n := len(cs.colNames)
	cVals, cleanup := valsToC(vals, n)
	defer cleanup()
	C.svdb_column_store_append_row(cs.getCStore(), (*C.svdb_value_t)(&cVals[0]), C.int(n))
}

// GetRow reads a row across all columns. Returns nil for deleted rows.
func (cs *ColumnStore) GetRow(idx int) []Value {
	if cs.cStore == nil {
		return nil
	}
	if C.svdb_column_store_is_deleted(cs.getCStore(), C.int(idx)) == 1 {
		return nil
	}
	n := len(cs.colNames)
	cVals := make([]C.svdb_value_t, n)
	var outCount C.int
	if C.svdb_column_store_get_row(cs.getCStore(), C.int(idx), (*C.svdb_value_t)(&cVals[0]), &outCount) == 0 {
		return nil
	}
	out := make([]Value, int(outCount))
	for i := 0; i < int(outCount); i++ {
		out[i] = toGoValue(cVals[i])
	}
	return out
}

// GetColumn returns a ColumnVector for the named column (nil if not found).
func (cs *ColumnStore) GetColumn(name string) *ColumnVector {
	i, ok := cs.colIdx[name]
	if !ok {
		return nil
	}
	return cs.GetColumnByIdx(i)
}

// GetColumnByIdx returns the ColumnVector at position colIdx by extracting from C++ store.
func (cs *ColumnStore) GetColumnByIdx(colIdx int) *ColumnVector {
	if cs.cStore == nil || colIdx < 0 || colIdx >= len(cs.colNames) {
		return nil
	}
	total := int(C.svdb_column_store_row_count(cs.getCStore()))
	vec := NewColumnVector(cs.colNames[colIdx], cs.colTypes[colIdx])
	n := len(cs.colNames)
	cVals := make([]C.svdb_value_t, n)
	var outCount C.int
	for i := 0; i < total; i++ {
		if C.svdb_column_store_is_deleted(cs.getCStore(), C.int(i)) == 1 {
			continue
		}
		if C.svdb_column_store_get_row(cs.getCStore(), C.int(i), (*C.svdb_value_t)(&cVals[0]), &outCount) == 1 && colIdx < int(outCount) {
			vec.Append(toGoValue(cVals[colIdx]))
		} else {
			vec.AppendNull()
		}
	}
	return vec
}

// DeleteRow marks a row as deleted.
func (cs *ColumnStore) DeleteRow(idx int) {
	if cs.cStore != nil {
		C.svdb_column_store_delete_row(cs.getCStore(), C.int(idx))
	}
}

// SetValue sets a specific cell value at (rowIdx, colIdx) in place.
func (cs *ColumnStore) SetValue(rowIdx, colIdx int, val Value) {
	if cs.cStore == nil {
		return
	}
	n := len(cs.colNames)
	cVals := make([]C.svdb_value_t, n)
	var outCount C.int
	// Read the entire row first.
	if C.svdb_column_store_get_row(cs.getCStore(), C.int(rowIdx), (*C.svdb_value_t)(&cVals[0]), &outCount) == 0 {
		return
	}
	// Overwrite the target column.
	if colIdx >= 0 && colIdx < int(outCount) {
		cs2 := goValueToC(val, &cVals[colIdx])
		if cs2 != nil {
			defer C.free(unsafe.Pointer(cs2))
		}
	}
	// Write back the updated row.
	C.svdb_column_store_update_row(cs.getCStore(), C.int(rowIdx), (*C.svdb_value_t)(&cVals[0]), C.int(int(outCount)))
}

// RowCount returns the total number of rows including deleted.
func (cs *ColumnStore) RowCount() int {
	if cs.cStore == nil {
		return 0
	}
	return int(C.svdb_column_store_row_count(cs.getCStore()))
}

// LiveCount returns the number of non-deleted rows.
func (cs *ColumnStore) LiveCount() int {
	if cs.cStore == nil {
		return 0
	}
	return int(C.svdb_column_store_live_count(cs.getCStore()))
}

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
	if cs.cStore == nil {
		return nil
	}
	total := int(C.svdb_column_store_row_count(cs.getCStore()))
	n := len(cs.colNames)
	out := make([]Row, 0, cs.LiveCount())
	cVals := make([]C.svdb_value_t, n)
	var outCount C.int
	for i := 0; i < total; i++ {
		if C.svdb_column_store_is_deleted(cs.getCStore(), C.int(i)) == 1 {
			continue
		}
		if C.svdb_column_store_get_row(cs.getCStore(), C.int(i), (*C.svdb_value_t)(&cVals[0]), &outCount) == 1 {
			vals := make([]Value, int(outCount))
			for j := 0; j < int(outCount); j++ {
				vals[j] = toGoValue(cVals[j])
			}
			out = append(out, NewRow(vals))
		}
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
		if pred(vec.Get(i)) {
			rb.Add(uint32(i))
		}
	}
	return rb
}

// Close frees the C++ column store resources.
func (cs *ColumnStore) Close() {
	if cs.cStore != nil {
		C.svdb_column_store_destroy(cs.getCStore())
		cs.cStore = nil
	}
}
