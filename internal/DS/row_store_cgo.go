//go:build SVDB_ENABLE_CGO_DS

package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "row_store.h"
#include "value.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// RowStore is a row-oriented in-memory store with tombstone-based deletion.
//
// This CGO version uses a dual-layer design:
//   - C++ row store (authoritative storage) — future batch/SIMD operations
//   - Go row slice (read cache) — O(1) random access with no CGO overhead
//
// Writes update both layers. Reads use the Go cache for maximum performance.
// The "Direct C Pointer" pattern is used (no Go→C++→Go callbacks).
type RowStore struct {
	cStore   unsafe.Pointer // *C.svdb_row_store_t (authoritative C++ storage)
	rows     []Row
	deleted  []bool
	columns  []string
	colTypes []ValueType
	colIdx   map[string]int
	live     int
}

// NewRowStore creates a RowStore backed by C++ row storage with a Go read cache.
func NewRowStore(columns []string, types []ValueType) *RowStore {
	idx := make(map[string]int, len(columns))
	for i, c := range columns {
		idx[c] = i
	}
	rs := &RowStore{columns: columns, colTypes: types, colIdx: idx}

	if len(columns) == 0 {
		return rs
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

	rs.cStore = unsafe.Pointer(C.svdb_row_store_create(
		(**C.char)(unsafe.Pointer(&cNames[0])),
		(*C.int)(&cTypes[0]),
		C.int(len(columns)),
	))
	return rs
}

func (rs *RowStore) getCStore() *C.svdb_row_store_t {
	return (*C.svdb_row_store_t)(rs.cStore)
}

// rowToC converts a Go Row to a C value array using valsToC for safe memory management.
func rowToC(row Row, n int) ([]C.svdb_value_t, func()) {
	vals := make([]Value, n)
	for i := 0; i < n; i++ {
		vals[i] = row.Get(i)
	}
	return valsToC(vals, n)
}

// Insert appends a row and returns its index, updating both C++ storage and Go cache.
func (rs *RowStore) Insert(row Row) int {
	rs.rows = append(rs.rows, row)
	rs.deleted = append(rs.deleted, false)
	rs.live++
	idx := len(rs.rows) - 1

	// Update C++ authoritative store.
	if rs.cStore != nil {
		n := len(rs.columns)
		cVals, cleanup := rowToC(row, n)
		defer cleanup()
		C.svdb_row_store_insert(rs.getCStore(), (*C.svdb_value_t)(&cVals[0]), C.int(n))
	}

	return idx
}

// Get returns the row at idx using the Go cache (O(1), no CGO overhead).
func (rs *RowStore) Get(idx int) Row {
	if idx < 0 || idx >= len(rs.rows) {
		return Row{}
	}
	return rs.rows[idx]
}

// Update replaces the row at idx in both C++ storage and Go cache.
func (rs *RowStore) Update(idx int, row Row) {
	if idx < 0 || idx >= len(rs.rows) {
		return
	}
	rs.rows[idx] = row
	if rs.cStore != nil {
		n := len(rs.columns)
		cVals, cleanup := rowToC(row, n)
		defer cleanup()
		C.svdb_row_store_update(rs.getCStore(), C.int(idx), (*C.svdb_value_t)(&cVals[0]), C.int(n))
	}
}

// Delete marks a row as deleted in both C++ storage and Go cache.
func (rs *RowStore) Delete(idx int) {
	if idx < 0 || idx >= len(rs.rows) || rs.deleted[idx] {
		return
	}
	rs.deleted[idx] = true
	rs.live--
	if rs.cStore != nil {
		C.svdb_row_store_delete(rs.getCStore(), C.int(idx))
	}
}

// Scan returns all non-deleted rows using the Go cache.
func (rs *RowStore) Scan() []Row {
	out := make([]Row, 0, rs.live)
	for i, row := range rs.rows {
		if !rs.deleted[i] {
			out = append(out, row)
		}
	}
	return out
}

// ScanIndices returns the indices of all non-deleted rows using the Go cache.
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

// ToColumnVectors converts the live rows into a slice of ColumnVectors using the Go cache.
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

// Close frees C++ row store resources.
func (rs *RowStore) Close() {
	if rs.cStore != nil {
		C.svdb_row_store_destroy(rs.getCStore())
		rs.cStore = nil
	}
}
