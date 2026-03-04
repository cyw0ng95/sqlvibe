// Package DS provides CGO wrappers for the C++ Data Storage layer.
//
// This file (ds_cgo.go) provides common documentation and type aliases
// used across all DS CGO wrappers. Actual implementations are in:
//   - overflow_cgo.go: PageManager callback registry and callbacks
//   - hybrid_store_cgo.go: Value conversions and scan result helpers
//   - btree_cgo.go: B-Tree CGO wrappers
//   - row_store_cgo.go: RowStore CGO wrappers
//   - column_store_cgo.go: ColumnStore CGO wrappers
//   - manager_cgo.go: Manager utility wrappers
//   - page_cgo.go: Page operation wrappers
//   - cache_cgo.go: Cache wrappers
//   - freelist_cgo.go: FreeList wrappers
//   - wal_cgo.go: WAL wrappers
//   - balance_cgo.go: Balance helpers
//   - btree_cursor_cgo.go: BTree cursor wrappers
//   - varint_cgo.go: VarInt helpers
package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include <stdint.h>
#include <stdlib.h>
*/
import "C"

// DS CGO Wrapper Architecture
//
// This package uses a dual-layer design for CGO wrappers:
//
// 1. C++ Layer (Authoritative Storage):
//    - src/core/DS/*.cpp implements all business logic
//    - Exposes C API via *_api.h headers
//    - Optimized for batch operations and SIMD
//
// 2. Go Layer (Read Cache + Type Mapping):
//    - internal/DS/*_cgo.go provides thin CGO wrappers
//    - Maintains Go read cache for O(1) access without CGO overhead
//    - Pure type conversion, no business logic
//
// CGO Call Patterns:
//
// 1. Direct C Pointer Pattern (preferred):
//    - Go allocates data, passes pointer to C++
//    - C++ operates directly on Go memory (pinned during call)
//    - No copying, minimal overhead
//    - Example: svdb_row_store_insert()
//
// 2. Callback Pattern (for polymorphism):
//    - Go exports callbacks (goPageRead, goPageWrite, etc.)
//    - C++ calls back to Go for page I/O
//    - Used for PageManager interface
//    - Overhead: ~5ns per callback
//
// 3. Result Allocation Pattern:
//    - C++ allocates result structures
//    - Go consumes and frees after use
//    - Example: svdb_scan_result_t
//
// Memory Management:
//
// - Go memory: Pinned during CGO calls, unpinned after
// - C memory: Freed by Go after CGO call completes
// - C++ memory: Managed by RAII, destroyed via explicit destroy() calls
//
// Thread Safety:
//
// - CGO calls are serialized by Go runtime
// - C++ code must be thread-safe for concurrent calls
// - PageManager registry uses RWMutex for concurrent reads

// ============================================================================
// Type Aliases and Constants
// ============================================================================

// Common CGO type aliases for documentation
type (
	// CInt32 is an alias for C int32_t
	CInt32 = C.int32_t
	// CUInt32 is an alias for C uint32_t
	CUInt32 = C.uint32_t
	// CInt64 is an alias for C int64_t
	CInt64 = C.int64_t
	// CSizeT is an alias for C size_t
	CSizeT = C.size_t
	// CBool is an alias for C int (used as boolean)
	CBool = C.int
)

// CGO call statistics (for debugging and profiling)
var (
	// cgoCallCount tracks total CGO calls made
	cgoCallCount uint64
	// cgoCallNanos tracks total time spent in CGO calls
	cgoCallNanos uint64
)

// incrementCGOCallCount increments the CGO call counter (for profiling)
func incrementCGOCallCount() {
	cgoCallCount++
}

// getCGOStats returns current CGO statistics
func getCGOStats() (calls uint64, nanos uint64) {
	return cgoCallCount, cgoCallNanos
}
