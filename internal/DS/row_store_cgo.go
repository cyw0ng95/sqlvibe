//go:build SVDB_ENABLE_CGO_DS

package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "row_store.h"
#include "columnar.h"
#include "value.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import "unsafe"

// RowStore is a row-oriented in-memory store with tombstone-based deletion.
// This CGO version delegates storage to the C++ row store engine (Direct C Pointer pattern).
type RowStore struct {
	cStore   unsafe.Pointer // *C.svdb_row_store_t
	columns  []string
	colTypes []ValueType
	colIdx   map[string]int
}

// NewRowStore creates a RowStore backed by C++ row storage.
func NewRowStore(columns []string, types []ValueType) *RowStore {
	idx := make(map[string]int, len(columns))
	for i, c := range columns {
		idx[c] = i
	}
	if len(columns) == 0 {
		return &RowStore{columns: columns, colTypes: types, colIdx: idx}
	}

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

	cStore := C.svdb_row_store_create(
		(**C.char)(unsafe.Pointer(&cNames[0])),
		(*C.int)(&cTypes[0]),
		C.int(len(columns)),
	)

	return &RowStore{
		cStore:   unsafe.Pointer(cStore),
		columns:  columns,
		colTypes: types,
		colIdx:   idx,
	}
}

func (rs *RowStore) getCStore() *C.svdb_row_store_t {
	return (*C.svdb_row_store_t)(rs.cStore)
}

// rowToC converts a Go Row to a C value array using valsToC for proper memory management.
// Returns the C array and cleanup function.
func rowToC(row Row, n int) ([]C.svdb_value_t, func()) {
	vals := make([]Value, n)
	for i := 0; i < n; i++ {
		vals[i] = row.Get(i)
	}
	return valsToC(vals, n)
}

// Insert appends a row and returns its index.
func (rs *RowStore) Insert(row Row) int {
	if rs.cStore == nil {
		return -1
	}
	n := len(rs.columns)
	cVals, cleanup := rowToC(row, n)
	defer cleanup()
	return int(C.svdb_row_store_insert(rs.getCStore(), (*C.svdb_value_t)(&cVals[0]), C.int(n)))
}

// Get returns the row at idx (including deleted rows).
func (rs *RowStore) Get(idx int) Row {
	if rs.cStore == nil {
		return Row{}
	}
	n := len(rs.columns)
	cVals := make([]C.svdb_value_t, n)
	var outCount C.int
	if C.svdb_row_store_get(rs.getCStore(), C.int(idx), (*C.svdb_value_t)(&cVals[0]), &outCount) == 0 {
		return Row{}
	}
	vals := make([]Value, int(outCount))
	for i := 0; i < int(outCount); i++ {
		vals[i] = toGoValue(cVals[i])
	}
	return NewRow(vals)
}

// Update replaces the row at idx.
func (rs *RowStore) Update(idx int, row Row) {
	if rs.cStore == nil {
		return
	}
	n := len(rs.columns)
	cVals, cleanup := rowToC(row, n)
	defer cleanup()
	C.svdb_row_store_update(rs.getCStore(), C.int(idx), (*C.svdb_value_t)(&cVals[0]), C.int(n))
}

// Delete marks a row as deleted (tombstone).
func (rs *RowStore) Delete(idx int) {
	if rs.cStore != nil {
		C.svdb_row_store_delete(rs.getCStore(), C.int(idx))
	}
}

// Scan returns all non-deleted rows.
func (rs *RowStore) Scan() []Row {
	if rs.cStore == nil {
		return nil
	}
	total := int(C.svdb_row_store_row_count(rs.getCStore()))
	n := len(rs.columns)
	out := make([]Row, 0, rs.LiveCount())
	cVals := make([]C.svdb_value_t, n)
	var outCount C.int
	for i := 0; i < total; i++ {
		if C.svdb_row_store_is_deleted(rs.getCStore(), C.int(i)) == 1 {
			continue
		}
		if C.svdb_row_store_get(rs.getCStore(), C.int(i), (*C.svdb_value_t)(&cVals[0]), &outCount) == 1 {
			vals := make([]Value, int(outCount))
			for j := 0; j < int(outCount); j++ {
				vals[j] = toGoValue(cVals[j])
			}
			out = append(out, NewRow(vals))
		}
	}
	return out
}

// ScanIndices returns the indices of all non-deleted rows.
func (rs *RowStore) ScanIndices() []int {
	if rs.cStore == nil {
		return nil
	}
	total := int(C.svdb_row_store_row_count(rs.getCStore()))
	out := make([]int, 0, rs.LiveCount())
	for i := 0; i < total; i++ {
		if C.svdb_row_store_is_deleted(rs.getCStore(), C.int(i)) == 0 {
			out = append(out, i)
		}
	}
	return out
}

// RowCount returns the total number of rows including deleted ones.
func (rs *RowStore) RowCount() int {
	if rs.cStore == nil {
		return 0
	}
	return int(C.svdb_row_store_row_count(rs.getCStore()))
}

// LiveCount returns the number of non-deleted rows.
func (rs *RowStore) LiveCount() int {
	if rs.cStore == nil {
		return 0
	}
	return int(C.svdb_row_store_live_count(rs.getCStore()))
}

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
	rows := rs.Scan()
	for _, row := range rows {
		for ci := range rs.columns {
			vecs[ci].Append(row.Get(ci))
		}
	}
	return vecs
}

// Close frees the C++ row store resources.
func (rs *RowStore) Close() {
	if rs.cStore != nil {
		C.svdb_row_store_destroy(rs.getCStore())
		rs.cStore = nil
	}
}
