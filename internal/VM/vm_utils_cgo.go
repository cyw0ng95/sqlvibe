package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "exec.h"
#include "query_engine.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// IsResultCacheEligible returns true if the SQL query result may be cached
// (i.e. the query has no non-deterministic side effects).
func IsResultCacheEligible(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_exec_is_result_cache_eligible(cs, C.size_t(len(sql))) != 0
}

// EstimateResultSize estimates memory in bytes needed for a result set.
func EstimateResultSize(numCols, numRows int) int {
	return int(C.svdb_exec_estimate_result_size(C.int(numCols), C.int(numRows)))
}

// ShouldUseColumnar returns true if columnar storage would be more efficient
// for the given number of columns and rows.
func ShouldUseColumnar(numCols, numRows int) bool {
	return C.svdb_exec_should_use_columnar(C.int(numCols), C.int(numRows)) != 0
}

// MaxInlineRows returns the threshold for inline vs heap row storage.
func MaxInlineRows() int {
	return int(C.svdb_exec_max_inline_rows())
}

// ComputeSQLHash computes a fast FNV-1a hash of the SQL string.
func ComputeSQLHash(sql string) uint64 {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return uint64(C.svdb_exec_compute_hash(cs, C.size_t(len(sql))))
}

// NormalizeWhitespace collapses runs of whitespace to a single space and
// trims leading/trailing whitespace. Returns the normalized SQL.
// Note: output is capped at 4096 bytes; SQL longer than ~4KB may be truncated,
// which is acceptable for this utility function (used for cache-key normalization).
func NormalizeWhitespace(sql string) string {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var buf [4096]C.char
	n := C.svdb_exec_normalize_whitespace(cs, C.size_t(len(sql)), &buf[0], C.int(len(buf)))
	if n <= 0 {
		return sql
	}
	return C.GoStringN(&buf[0], n)
}

// ClassifyQuery classifies a SQL statement.
// Returns: 0=unknown, 1=SELECT, 2=INSERT, 3=UPDATE, 4=DELETE,
// 5=CREATE, 6=DROP, 7=ALTER, 8=BEGIN, 9=COMMIT, 10=ROLLBACK, 11=PRAGMA.
func ClassifyQuery(sql string) int {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return int(C.svdb_qe_classify_query(cs, C.size_t(len(sql))))
}

// ExtractTableName extracts the main table name from a DML/DDL statement.
// Returns an empty string if not found.
// Note: output is capped at 4096 bytes; table names longer than ~4KB may be truncated
// (not a realistic concern in practice, but documented for completeness).
func ExtractTableName(sql string) string {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var buf [4096]C.char
	n := C.svdb_qe_extract_table_name(cs, C.size_t(len(sql)), &buf[0], C.int(len(buf)))
	if n <= 0 {
		return ""
	}
	return C.GoStringN(&buf[0], n)
}

// IsReadOnlyQuery returns true if the query is SELECT or PRAGMA (read-only).
func IsReadOnlyQuery(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_qe_is_read_only(cs, C.size_t(len(sql))) != 0
}

// IsTransactionQuery returns true if the query is BEGIN/COMMIT/ROLLBACK/SAVEPOINT.
func IsTransactionQuery(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_qe_is_transaction(cs, C.size_t(len(sql))) != 0
}

// NeedsSchema returns true if the query requires schema lookup.
func NeedsSchema(sql string) bool {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	return C.svdb_qe_needs_schema(cs, C.size_t(len(sql))) != 0
}

// StripComments removes SQL line comments (--) and block comments (/* */).
// Returns the cleaned SQL string.
// Note: output is capped at 4096 bytes; SQL longer than ~4KB may be truncated,
// which is acceptable for this utility function.
func StripComments(sql string) string {
	cs := C.CString(sql)
	defer C.free(unsafe.Pointer(cs))
	var buf [4096]C.char
	n := C.svdb_qe_strip_comments(cs, C.size_t(len(sql)), &buf[0], C.int(len(buf)))
	if n <= 0 {
		return sql
	}
	return C.GoStringN(&buf[0], n)
}
