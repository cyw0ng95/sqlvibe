package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "hybrid_store_api.h"
#include <stdlib.h>
#include <string.h>

// Forward declarations for Go callback exports (CGO generates these without const)
extern svdb_value_t goGetRowValue(uint32_t row_idx, int32_t col_idx, void* user_data);
extern int32_t goCompareValues(svdb_value_t* a, svdb_value_t* b);
*/
import "C"
import (
	"unsafe"
)

// CIndexEngine is a CGO wrapper around the C++ IndexEngine implementation.
type CIndexEngine struct {
	ptr *C.svdb_index_engine_t
}

// NewCIndexEngine creates a new C++ index engine.
func NewCIndexEngine() *CIndexEngine {
	return &CIndexEngine{
		ptr: C.svdb_index_engine_create(),
	}
}

// Destroy frees the C++ index engine.
func (ie *CIndexEngine) Destroy() {
	if ie.ptr != nil {
		C.svdb_index_engine_destroy(ie.ptr)
		ie.ptr = nil
	}
}

// AddBitmapIndex creates a bitmap index for colName.
func (ie *CIndexEngine) AddBitmapIndex(colName string) {
	cColName := C.CString(colName)
	defer C.free(unsafe.Pointer(cColName))
	C.svdb_index_engine_add_bitmap(ie.ptr, cColName)
}

// AddSkipListIndex creates a skip-list index for colName.
func (ie *CIndexEngine) AddSkipListIndex(colName string) {
	cColName := C.CString(colName)
	defer C.free(unsafe.Pointer(cColName))
	C.svdb_index_engine_add_skiplist(ie.ptr, cColName)
}

// HasBitmapIndex reports whether a bitmap index exists for colName.
func (ie *CIndexEngine) HasBitmapIndex(colName string) bool {
	cColName := C.CString(colName)
	defer C.free(unsafe.Pointer(cColName))
	return C.svdb_index_engine_has_bitmap(ie.ptr, cColName) != 0
}

// HasSkipListIndex reports whether a skip-list index exists for colName.
func (ie *CIndexEngine) HasSkipListIndex(colName string) bool {
	cColName := C.CString(colName)
	defer C.free(unsafe.Pointer(cColName))
	return C.svdb_index_engine_has_skiplist(ie.ptr, cColName) != 0
}

// IndexRow updates indexes for a newly inserted row.
func (ie *CIndexEngine) IndexRow(rowIdx uint32, colName string, val Value) {
	cColName := C.CString(colName)
	defer C.free(unsafe.Pointer(cColName))
	cVal := goValToC(val)
	C.svdb_index_engine_index_row(ie.ptr, C.uint32_t(rowIdx), cColName, &cVal)
}

// UnindexRow removes a row from all indexes.
func (ie *CIndexEngine) UnindexRow(rowIdx uint32, colName string, val Value) {
	cColName := C.CString(colName)
	defer C.free(unsafe.Pointer(cColName))
	cVal := goValToC(val)
	C.svdb_index_engine_unindex_row(ie.ptr, C.uint32_t(rowIdx), cColName, &cVal)
}

// LookupEqual returns row indices where colName == val.
func (ie *CIndexEngine) LookupEqual(colName string, val Value, numCols int32) *C.svdb_scan_result_t {
	cColName := C.CString(colName)
	defer C.free(unsafe.Pointer(cColName))
	cVal := goValToC(val)
	return C.svdb_index_lookup_equal(ie.ptr, cColName, &cVal, C.int32_t(numCols))
}

// LookupRange returns row indices where colName is in [lo, hi].
func (ie *CIndexEngine) LookupRange(colName string, lo, hi Value, numCols int32, inclusive bool) *C.svdb_scan_result_t {
	cColName := C.CString(colName)
	defer C.free(unsafe.Pointer(cColName))
	cLo := goValToC(lo)
	cHi := goValToC(hi)
	cInclusive := C.int(0)
	if inclusive {
		cInclusive = C.int(1)
	}
	return C.svdb_index_lookup_range(ie.ptr, cColName, &cLo, &cHi, C.int32_t(numCols), cInclusive)
}

// goValToC converts a Go Value to a C svdb_value_t.
func goValToC(v Value) C.svdb_value_t {
	var cv C.svdb_value_t
	switch v.Type {
	case TypeNull:
		cv.val_type = C.SVDB_VAL_NULL
	case TypeInt:
		cv.val_type = C.SVDB_VAL_INT
		cv.int_val = C.int64_t(v.Int)
	case TypeFloat:
		cv.val_type = C.SVDB_VAL_FLOAT
		cv.float_val = C.double(v.Float)
	case TypeString:
		cv.val_type = C.SVDB_VAL_TEXT
		if v.Str != "" {
			cv.str_data = C.CString(v.Str)
			cv.str_len = C.size_t(len(v.Str))
		}
	case TypeBytes:
		cv.val_type = C.SVDB_VAL_BLOB
		if len(v.Bytes) > 0 {
			cv.str_data = (*C.char)(C.CBytes(v.Bytes))
			cv.str_len = C.size_t(len(v.Bytes))
		}
	default:
		cv.val_type = C.SVDB_VAL_NULL
	}
	return cv
}

// cValToGo converts a C svdb_value_t to a Go Value.
func cValToGo(cv C.svdb_value_t) Value {
	var v Value
	switch cv.val_type {
	case C.SVDB_VAL_NULL:
		v.Type = TypeNull
	case C.SVDB_VAL_INT:
		v.Type = TypeInt
		v.Int = int64(cv.int_val)
	case C.SVDB_VAL_FLOAT:
		v.Type = TypeFloat
		v.Float = float64(cv.float_val)
	case C.SVDB_VAL_TEXT:
		v.Type = TypeString
		if cv.str_data != nil {
			v.Str = C.GoStringN(cv.str_data, C.int(cv.str_len))
		}
	case C.SVDB_VAL_BLOB:
		v.Type = TypeBytes
		if cv.str_data != nil && cv.str_len > 0 {
			v.Bytes = C.GoBytes(unsafe.Pointer(cv.str_data), C.int(cv.str_len))
		}
	default:
		v.Type = TypeNull
	}
	return v
}

// CScanWithFilter performs a filtered scan using C++ comparison.
func CScanWithFilter(
	rowStore unsafe.Pointer,
	getValueFn C.svdb_get_row_value_fn,
	rowIndices []int32,
	numCols int32,
	filterColIdx int32,
	filterVal Value,
	op string,
	cmpFn C.svdb_compare_values_fn,
) *C.svdb_scan_result_t {
	if len(rowIndices) == 0 {
		return C.svdb_scan_result_alloc(0, C.int32_t(numCols))
	}

	cRowIndices := C.malloc(C.size_t(len(rowIndices)) * C.size_t(unsafe.Sizeof(rowIndices[0])))
	if cRowIndices == nil {
		return nil
	}
	copy((*[1 << 30]int32)(cRowIndices)[:len(rowIndices):len(rowIndices)], rowIndices)

	cOp := C.CString(op)
	defer C.free(unsafe.Pointer(cOp))

	cVal := goValToC(filterVal)

	result := C.svdb_scan_with_filter(
		rowStore,
		getValueFn,
		(*C.int32_t)(cRowIndices),
		C.int32_t(len(rowIndices)),
		C.int32_t(numCols),
		C.int32_t(filterColIdx),
		&cVal,
		cOp,
		cmpFn,
	)

	C.free(cRowIndices)
	return result
}

// CScanResultToValues converts a C scan result to Go [][]Value.
func CScanResultToValues(result *C.svdb_scan_result_t) [][]Value {
	if result == nil || result.num_rows <= 0 || result.num_cols <= 0 {
		return nil
	}

	numRows := int(result.num_rows)
	numCols := int(result.num_cols)
	values := (*[1 << 30]C.svdb_value_t)(unsafe.Pointer(result.values))[:numRows*numCols : numRows*numCols]

	out := make([][]Value, numRows)
	for i := 0; i < numRows; i++ {
		row := make([]Value, numCols)
		for j := 0; j < numCols; j++ {
			row[j] = cValToGo(values[i*numCols+j])
		}
		out[i] = row
	}
	return out
}

// CScanResultFree frees a C scan result.
func CScanResultFree(result *C.svdb_scan_result_t) {
	if result != nil {
		C.svdb_scan_result_free(result)
	}
}

//export goGetRowValue
func goGetRowValue(rowIdx C.uint32_t, colIdx C.int32_t, userData unsafe.Pointer) C.svdb_value_t {
	type getRowValueFn func(uint32, int32) Value
	fn := *(*getRowValueFn)(userData)
	v := fn(uint32(rowIdx), int32(colIdx))
	return goValToC(v)
}

//export goCompareValues
func goCompareValues(a, b *C.svdb_value_t) C.int32_t {
	va := cValToGo(*a)
	vb := cValToGo(*b)
	cmp := Compare(va, vb)
	return C.int32_t(cmp)
}
