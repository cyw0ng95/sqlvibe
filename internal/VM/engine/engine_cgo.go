package engine

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../../src/core/VM/engine
#include "engine_api.h"
#include <stdlib.h>
#include <string.h>

// Forward declarations for Go callback exports (CGO generates these without const)
extern int32_t goRowPredicate(svdb_engine_row_t* row, void* user_data);
extern char* goRowKeyFn(svdb_engine_row_t* row, void* user_data);
extern int32_t goJoinPredicate(svdb_engine_row_t* merged, void* user_data);
*/
import "C"
import (
	"unsafe"
)

// maxCSlice is the upper-bound capacity used when constructing Go slice headers
// over C-allocated arrays via unsafe.Pointer.  268,435,456 (2^28) is chosen as
// a conservatively large value that fits in a 32-bit signed integer and is far
// larger than any row array we will ever allocate, while staying well below any
// reasonable address-space limit (arrays are individually malloc'd at size n).
const maxCSlice = 1 << 28

// goValToC converts a Go interface{} to a C svdb_engine_value_t.
// Text/blob str_data is heap-allocated with malloc; the caller or the C
// svdb_engine_rows_free will free it.
func goValToC(v interface{}) C.svdb_value_t {
	var cv C.svdb_value_t
	if v == nil {
		cv.val_type = C.SVDB_VAL_NULL
		return cv
	}
	switch x := v.(type) {
	case int64:
		cv.val_type = C.SVDB_VAL_INT
		cv.int_val = C.int64_t(x)
	case int:
		cv.val_type = C.SVDB_VAL_INT
		cv.int_val = C.int64_t(x)
	case int32:
		cv.val_type = C.SVDB_VAL_INT
		cv.int_val = C.int64_t(x)
	case float64:
		cv.val_type = C.SVDB_VAL_FLOAT
		cv.float_val = C.double(x)
	case float32:
		cv.val_type = C.SVDB_VAL_FLOAT
		cv.float_val = C.double(x)
	case string:
		cv.val_type = C.SVDB_VAL_TEXT
		cv.str_data = C.CString(x)
		cv.str_len = C.size_t(len(x))
	case []byte:
		cv.val_type = C.SVDB_VAL_BLOB
		if len(x) > 0 {
			cv.str_data = (*C.char)(C.CBytes(x))
			cv.str_len = C.size_t(len(x))
		}
	case bool:
		cv.val_type = C.SVDB_VAL_INT
		if x {
			cv.int_val = 1
		}
	default:
		cv.val_type = C.SVDB_VAL_NULL
	}
	return cv
}

// cValToGo converts a C svdb_engine_value_t to a Go interface{}.
func cValToGo(cv C.svdb_value_t) interface{} {
	switch cv.val_type {
	case C.SVDB_VAL_NULL:
		return nil
	case C.SVDB_VAL_INT:
		return int64(cv.int_val)
	case C.SVDB_VAL_FLOAT:
		return float64(cv.float_val)
	case C.SVDB_VAL_TEXT:
		if cv.str_data == nil {
			return ""
		}
		return C.GoStringN(cv.str_data, C.int(cv.str_len))
	case C.SVDB_VAL_BLOB:
		if cv.str_data == nil {
			return []byte{}
		}
		return C.GoBytes(unsafe.Pointer(cv.str_data), C.int(cv.str_len))
	}
	return nil
}

// goFillCRow fills a pre-allocated (calloc'd) svdb_engine_row_t from a Go Row.
func goFillCRow(crow *C.svdb_engine_row_t, r Row) {
	n := len(r)
	crow.num_cols = C.int32_t(n)
	if n == 0 {
		return
	}
	ptrSize := unsafe.Sizeof((*C.char)(nil))
	valSize := C.size_t(unsafe.Sizeof(C.svdb_value_t{}))
	crow.col_names = (**C.char)(C.malloc(C.size_t(n) * C.size_t(ptrSize)))
	crow.vals = (*C.svdb_value_t)(C.malloc(C.size_t(n) * valSize))

	names := (*[maxCSlice]*C.char)(unsafe.Pointer(crow.col_names))[:n:n]
	vals := (*[maxCSlice]C.svdb_value_t)(unsafe.Pointer(crow.vals))[:n:n]

	i := 0
	for k, v := range r {
		names[i] = C.CString(k)
		vals[i] = goValToC(v)
		i++
	}
}

// goRowsToC converts a Go []Row to a C svdb_engine_rows_t*.
// The caller must call C.svdb_engine_rows_free on the result.
func goRowsToC(rows []Row) *C.svdb_engine_rows_t {
	n := len(rows)
	crows := C.svdb_engine_rows_alloc(C.int32_t(n))
	if crows == nil {
		return nil
	}
	if n > 0 {
		rowSlice := (*[maxCSlice]C.svdb_engine_row_t)(unsafe.Pointer(crows.rows))[:n:n]
		for i, r := range rows {
			goFillCRow(&rowSlice[i], r)
		}
	}
	return crows
}

// cRowToGo converts a C svdb_engine_row_t* to a Go Row.
func cRowToGo(crow *C.svdb_engine_row_t) Row {
	n := int(crow.num_cols)
	r := make(Row, n)
	if n == 0 {
		return r
	}
	names := (*[maxCSlice]*C.char)(unsafe.Pointer(crow.col_names))[:n:n]
	vals := (*[maxCSlice]C.svdb_value_t)(unsafe.Pointer(crow.vals))[:n:n]
	for i := 0; i < n; i++ {
		key := C.GoString(names[i])
		r[key] = cValToGo(vals[i])
	}
	return r
}

// cRowsToGo converts a C svdb_engine_rows_t* to a Go []Row.
func cRowsToGo(crows *C.svdb_engine_rows_t) []Row {
	if crows == nil || crows.num_rows == 0 {
		return nil
	}
	n := int(crows.num_rows)
	rowSlice := (*[maxCSlice]C.svdb_engine_row_t)(unsafe.Pointer(crows.rows))[:n:n]
	rows := make([]Row, n)
	for i := 0; i < n; i++ {
		rows[i] = cRowToGo(&rowSlice[i])
	}
	return rows
}

// CApplyLimitOffset applies LIMIT and OFFSET using the C++ implementation.
func CApplyLimitOffset(rows []Row, limit, offset int) []Row {
	crows := goRowsToC(rows)
	if crows == nil {
		return nil
	}
	defer C.svdb_engine_rows_free(crows)

	out := C.svdb_engine_apply_limit_offset(crows, C.int32_t(limit), C.int32_t(offset))
	defer C.svdb_engine_rows_free(out)
	return cRowsToGo(out)
}

// CColNames returns deduplicated column names using the C++ implementation.
func CColNames(rows []Row) []string {
	crows := goRowsToC(rows)
	if crows == nil {
		return nil
	}
	defer C.svdb_engine_rows_free(crows)

	count := C.svdb_engine_col_names(crows, nil, 0)
	if count <= 0 {
		return nil
	}
	n := int(count)
	// Allocate a C array of char* pointers.
	ptrSize := C.size_t(unsafe.Sizeof((*C.char)(nil)))
	buf := (**C.char)(C.malloc(C.size_t(n) * ptrSize))
	defer C.free(unsafe.Pointer(buf))

	C.svdb_engine_col_names(crows, buf, count)
	ptrs := (*[maxCSlice]*C.char)(unsafe.Pointer(buf))[:n:n]
	names := make([]string, n)
	for i := 0; i < n; i++ {
		names[i] = C.GoString(ptrs[i])
	}
	return names
}

// CMergeRows merges two rows using the C++ implementation.
func CMergeRows(a, b Row) Row {
	ca := goFillNewCRow(a)
	cb := goFillNewCRow(b)
	defer C.svdb_engine_row_free(ca)
	defer C.svdb_engine_row_free(cb)

	out := C.svdb_engine_merge_rows(ca, cb)
	if out == nil {
		return make(Row)
	}
	r := cRowToGo(out)
	C.svdb_engine_row_free(out)
	return r
}

// CMergeRowsWithAlias merges two rows with alias qualification using C++.
func CMergeRowsWithAlias(a Row, aliasA string, b Row, aliasB string) Row {
	ca := goFillNewCRow(a)
	cb := goFillNewCRow(b)
	defer C.svdb_engine_row_free(ca)
	defer C.svdb_engine_row_free(cb)

	cAliasA := C.CString(aliasA)
	cAliasB := C.CString(aliasB)
	defer C.free(unsafe.Pointer(cAliasA))
	defer C.free(unsafe.Pointer(cAliasB))

	out := C.svdb_engine_merge_rows_alias(ca, cAliasA, cb, cAliasB)
	if out == nil {
		return make(Row)
	}
	r := cRowToGo(out)
	C.svdb_engine_row_free(out)
	return r
}

// goFillNewCRow allocates a standalone svdb_engine_row_t* from a Go Row.
// The caller must call C.svdb_engine_row_free on the result.
func goFillNewCRow(r Row) *C.svdb_engine_row_t {
	n := len(r)
	crow := C.svdb_engine_row_alloc(C.int32_t(n))
	if crow == nil {
		return nil
	}
	if n > 0 {
		names := (*[maxCSlice]*C.char)(unsafe.Pointer(crow.col_names))[:n:n]
		vals := (*[maxCSlice]C.svdb_value_t)(unsafe.Pointer(crow.vals))[:n:n]
		i := 0
		for k, v := range r {
			names[i] = C.CString(k)
			vals[i] = goValToC(v)
			i++
		}
	}
	return crow
}

// CCrossJoin returns the Cartesian product using the C++ implementation.
func CCrossJoin(left, right []Row) []Row {
	cleft := goRowsToC(left)
	cright := goRowsToC(right)
	defer C.svdb_engine_rows_free(cleft)
	defer C.svdb_engine_rows_free(cright)

	out := C.svdb_engine_cross_join(cleft, cright)
	defer C.svdb_engine_rows_free(out)
	return cRowsToGo(out)
}

// CCountRows counts non-nil column values using the C++ implementation.
func CCountRows(rows []Row, col string) int64 {
	crows := goRowsToC(rows)
	if crows == nil {
		return 0
	}
	defer C.svdb_engine_rows_free(crows)

	cCol := C.CString(col)
	defer C.free(unsafe.Pointer(cCol))
	return int64(C.svdb_engine_count_rows(crows, cCol))
}

// CSortKey is the Go equivalent of svdb_engine_sort_key_t.
type CSortKey struct {
	ColName   string
	Order     int32 // 0=ASC 1=DESC
	NullOrder int32 // 0=NULLS FIRST 1=NULLS LAST
}

// CSortRows sorts rows using the C++ implementation.
func CSortRows(rows []Row, keys []CSortKey) []Row {
	crows := goRowsToC(rows)
	if crows == nil {
		return nil
	}
	defer C.svdb_engine_rows_free(crows)

	var cKeys *C.svdb_engine_sort_key_t
	cKeySlice := make([]C.svdb_engine_sort_key_t, len(keys))
	cKeyNames := make([]*C.char, len(keys))
	for i, k := range keys {
		cKeyNames[i] = C.CString(k.ColName)
		cKeySlice[i].col_name = cKeyNames[i]
		cKeySlice[i].order = C.int32_t(k.Order)
		cKeySlice[i].null_order = C.int32_t(k.NullOrder)
	}
	defer func() {
		// cKeyNames is fully populated by the loop above and is safe to free
		// even if svdb_engine_sort_rows panics.
		for _, p := range cKeyNames {
			C.free(unsafe.Pointer(p))
		}
	}()
	if len(cKeySlice) > 0 {
		cKeys = &cKeySlice[0]
	}

	out := C.svdb_engine_sort_rows(crows, cKeys, C.int32_t(len(keys)))
	defer C.svdb_engine_rows_free(out)
	return cRowsToGo(out)
}

// CReverseRows reverses rows using the C++ implementation.
func CReverseRows(rows []Row) []Row {
	crows := goRowsToC(rows)
	if crows == nil {
		return nil
	}
	defer C.svdb_engine_rows_free(crows)

	out := C.svdb_engine_reverse_rows(crows)
	defer C.svdb_engine_rows_free(out)
	return cRowsToGo(out)
}

// CExistsRows returns true if rows is non-empty using the C++ implementation.
func CExistsRows(rows []Row) bool {
	crows := goRowsToC(rows)
	if crows == nil {
		return false
	}
	defer C.svdb_engine_rows_free(crows)
	return C.svdb_engine_exists_rows(crows) != 0
}

// CInRows checks whether value appears in rows[col] using the C++ implementation.
func CInRows(value interface{}, rows []Row, col string) bool {
	if value == nil {
		return false
	}
	crows := goRowsToC(rows)
	if crows == nil {
		return false
	}
	defer C.svdb_engine_rows_free(crows)

	cv := goValToC(value)
	// Free any string data allocated by goValToC after the call.
	defer freeValData(&cv)

	cCol := C.CString(col)
	defer C.free(unsafe.Pointer(cCol))
	return C.svdb_engine_in_rows(&cv, crows, cCol) != 0
}

// CNotInRows returns true when value does not appear in rows[col].
func CNotInRows(value interface{}, rows []Row, col string) bool {
	if value == nil {
		return false
	}
	crows := goRowsToC(rows)
	if crows == nil {
		return true
	}
	defer C.svdb_engine_rows_free(crows)

	cv := goValToC(value)
	defer freeValData(&cv)

	cCol := C.CString(col)
	defer C.free(unsafe.Pointer(cCol))
	return C.svdb_engine_not_in_rows(&cv, crows, cCol) != 0
}

// freeValData frees heap-allocated string data in a C value produced by goValToC.
func freeValData(cv *C.svdb_value_t) {
	if (cv.val_type == C.SVDB_VAL_TEXT || cv.val_type == C.SVDB_VAL_BLOB) &&
		cv.str_data != nil {
		C.free(unsafe.Pointer(cv.str_data))
		cv.str_data = nil
	}
}

// CRowNumbers returns 1-based row numbers using the C++ implementation.
func CRowNumbers(n int) []int64 {
	if n <= 0 {
		return nil
	}
	ptr := C.svdb_engine_row_numbers(C.int32_t(n))
	if ptr == nil {
		return nil
	}
	defer C.free(unsafe.Pointer(ptr))
	nums := (*[maxCSlice]C.int64_t)(unsafe.Pointer(ptr))[:n:n]
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		out[i] = int64(nums[i])
	}
	return out
}

// CRanks returns RANK() values for rows sorted by col using the C++ implementation.
func CRanks(rows []Row, col string) []int64 {
	n := len(rows)
	if n == 0 {
		return nil
	}
	crows := goRowsToC(rows)
	if crows == nil {
		return nil
	}
	defer C.svdb_engine_rows_free(crows)

	cCol := C.CString(col)
	defer C.free(unsafe.Pointer(cCol))

	ptr := C.svdb_engine_ranks(crows, cCol)
	if ptr == nil {
		return nil
	}
	defer C.free(unsafe.Pointer(ptr))
	raw := (*[maxCSlice]C.int64_t)(unsafe.Pointer(ptr))[:n:n]
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		out[i] = int64(raw[i])
	}
	return out
}

// CDenseRanks returns DENSE_RANK() values using the C++ implementation.
func CDenseRanks(rows []Row, col string) []int64 {
	n := len(rows)
	if n == 0 {
		return nil
	}
	crows := goRowsToC(rows)
	if crows == nil {
		return nil
	}
	defer C.svdb_engine_rows_free(crows)

	cCol := C.CString(col)
	defer C.free(unsafe.Pointer(cCol))

	ptr := C.svdb_engine_dense_ranks(crows, cCol)
	if ptr == nil {
		return nil
	}
	defer C.free(unsafe.Pointer(ptr))
	raw := (*[maxCSlice]C.int64_t)(unsafe.Pointer(ptr))[:n:n]
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		out[i] = int64(raw[i])
	}
	return out
}

// CFilterRows filters rows using the C++ implementation.
// pred is a Go closure that receives each row and returns true to keep it.
func CFilterRows(rows []Row, pred func(Row) bool) []Row {
	if len(rows) == 0 {
		return nil
	}
	crows := goRowsToC(rows)
	if crows == nil {
		return nil
	}
	defer C.svdb_engine_rows_free(crows)

	// Pass predicate via pointer (Go closure)
	predPtr := unsafe.Pointer(&pred)
	out := C.svdb_engine_filter_rows(crows, (*[0]byte)(unsafe.Pointer(C.goRowPredicate)), predPtr)
	defer C.svdb_engine_rows_free(out)
	return cRowsToGo(out)
}

// CApplyDistinct removes duplicate rows using the C++ implementation.
// keyFn computes a deduplication key for each row.
func CApplyDistinct(rows []Row, keyFn func(Row) string) []Row {
	if len(rows) == 0 {
		return nil
	}
	crows := goRowsToC(rows)
	if crows == nil {
		return nil
	}
	defer C.svdb_engine_rows_free(crows)

	keyFnPtr := unsafe.Pointer(&keyFn)
	out := C.svdb_engine_apply_distinct(crows, (*[0]byte)(unsafe.Pointer(C.goRowKeyFn)), keyFnPtr)
	defer C.svdb_engine_rows_free(out)
	return cRowsToGo(out)
}

// CInnerJoin performs inner join using the C++ implementation.
// pred receives merged rows and returns true to include them.
func CInnerJoin(left, right []Row, pred func(Row) bool) []Row {
	if len(left) == 0 || len(right) == 0 {
		return nil
	}
	cleft := goRowsToC(left)
	cright := goRowsToC(right)
	defer C.svdb_engine_rows_free(cleft)
	defer C.svdb_engine_rows_free(cright)

	predPtr := unsafe.Pointer(&pred)
	out := C.svdb_engine_inner_join(cleft, cright, (*[0]byte)(unsafe.Pointer(C.goJoinPredicate)), predPtr)
	defer C.svdb_engine_rows_free(out)
	return cRowsToGo(out)
}

// CLeftOuterJoin performs left outer join using the C++ implementation.
// pred receives merged rows and returns true to include them.
// rightCols lists column names in right rows for NULL padding.
func CLeftOuterJoin(left, right []Row, pred func(Row) bool, rightCols []string) []Row {
	if len(left) == 0 {
		return nil
	}
	cleft := goRowsToC(left)
	defer C.svdb_engine_rows_free(cleft)

	if len(right) == 0 {
		// No right rows — return left with NULL right columns
		cRightCols := make([]*C.char, len(rightCols))
		for i, col := range rightCols {
			cRightCols[i] = C.CString(col)
			defer C.free(unsafe.Pointer(cRightCols[i]))
		}
		var cCols **C.char
		if len(cRightCols) > 0 {
			cCols = &cRightCols[0]
		}
		out := C.svdb_engine_left_outer_join(cleft, nil, nil, nil, cCols, C.int32_t(len(rightCols)))
		defer C.svdb_engine_rows_free(out)
		return cRowsToGo(out)
	}

	cright := goRowsToC(right)
	defer C.svdb_engine_rows_free(cright)

	cRightCols := make([]*C.char, len(rightCols))
	for i, col := range rightCols {
		cRightCols[i] = C.CString(col)
		defer C.free(unsafe.Pointer(cRightCols[i]))
	}
	var cCols **C.char
	if len(cRightCols) > 0 {
		cCols = &cRightCols[0]
	}

	predPtr := unsafe.Pointer(&pred)
	out := C.svdb_engine_left_outer_join(cleft, cright, (*[0]byte)(unsafe.Pointer(C.goJoinPredicate)), predPtr, cCols, C.int32_t(len(rightCols)))
	defer C.svdb_engine_rows_free(out)
	return cRowsToGo(out)
}

// CGroupRows groups rows by key using the C++ implementation.
// keyFn computes a group key for each row.
// Returns one row per group with "group_key" column.
func CGroupRows(rows []Row, keyFn func(Row) string) []Row {
	if len(rows) == 0 {
		return nil
	}
	crows := goRowsToC(rows)
	if crows == nil {
		return nil
	}
	defer C.svdb_engine_rows_free(crows)

	keyFnPtr := unsafe.Pointer(&keyFn)
	out := C.svdb_engine_group_rows(crows, (*[0]byte)(unsafe.Pointer(C.goRowKeyFn)), keyFnPtr)
	defer C.svdb_engine_rows_free(out)
	return cRowsToGo(out)
}

// CSumRows computes SUM of a column using the C++ implementation.
// Returns float64 for compatibility with SQLite SUM behaviour.
func CSumRows(rows []Row, col string) interface{} {
	if len(rows) == 0 {
		return nil
	}
	crows := goRowsToC(rows)
	if crows == nil {
		return nil
	}
	defer C.svdb_engine_rows_free(crows)

	cCol := C.CString(col)
	defer C.free(unsafe.Pointer(cCol))

	cv := C.svdb_engine_sum_rows(crows, cCol)
	return cValToGo(cv)
}

// CAvgRows computes AVG of a column using the C++ implementation.
func CAvgRows(rows []Row, col string) float64 {
	if len(rows) == 0 {
		return 0.0
	}
	crows := goRowsToC(rows)
	if crows == nil {
		return 0.0
	}
	defer C.svdb_engine_rows_free(crows)

	cCol := C.CString(col)
	defer C.free(unsafe.Pointer(cCol))

	return float64(C.svdb_engine_avg_rows(crows, cCol))
}

// CMinRows finds minimum value in a column using the C++ implementation.
// Returns nil if all values are NULL or rows is empty.
func CMinRows(rows []Row, col string) interface{} {
	if len(rows) == 0 {
		return nil
	}
	crows := goRowsToC(rows)
	if crows == nil {
		return nil
	}
	defer C.svdb_engine_rows_free(crows)

	cCol := C.CString(col)
	defer C.free(unsafe.Pointer(cCol))

	cv := C.svdb_engine_min_rows(crows, cCol)
	return cValToGo(cv)
}

// CMaxRows finds maximum value in a column using the C++ implementation.
// Returns nil if all values are NULL or rows is empty.
func CMaxRows(rows []Row, col string) interface{} {
	if len(rows) == 0 {
		return nil
	}
	crows := goRowsToC(rows)
	if crows == nil {
		return nil
	}
	defer C.svdb_engine_rows_free(crows)

	cCol := C.CString(col)
	defer C.free(unsafe.Pointer(cCol))

	cv := C.svdb_engine_max_rows(crows, cCol)
	return cValToGo(cv)
}

//export goRowPredicate
func goRowPredicate(row *C.svdb_engine_row_t, userData unsafe.Pointer) C.int32_t {
	type predicateFunc func(Row) bool
	pred := *(*predicateFunc)(userData)
	r := cRowToGo(row)
	if pred(r) {
		return 1
	}
	return 0
}

//export goRowKeyFn
func goRowKeyFn(row *C.svdb_engine_row_t, userData unsafe.Pointer) *C.char {
	type keyFunc func(Row) string
	keyFn := *(*keyFunc)(userData)
	r := cRowToGo(row)
	key := keyFn(r)
	return C.CString(key)
}

//export goJoinPredicate
func goJoinPredicate(merged *C.svdb_engine_row_t, userData unsafe.Pointer) C.int32_t {
	type joinPredFunc func(Row) bool
	pred := *(*joinPredFunc)(userData)
	r := cRowToGo(merged)
	if pred(r) {
		return 1
	}
	return 0
}
