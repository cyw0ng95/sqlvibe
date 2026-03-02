package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "aggregate_engine_api.h"
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// AggFunc mirrors the C++ AggFunc enum.
const (
	AggFuncCount       = 1
	AggFuncSum         = 2
	AggFuncAvg         = 3
	AggFuncMin         = 4
	AggFuncMax         = 5
	AggFuncGroupConcat = 6
)

// CAggregateEngine wraps the C++ AggregateEngine for group aggregation.
type CAggregateEngine struct {
	ptr unsafe.Pointer // opaque *SVDB_VM_AggregateEngine
}

// NewCAggregateEngine creates a new C++ aggregate engine.
func NewCAggregateEngine() *CAggregateEngine {
	a := &CAggregateEngine{ptr: C.SVDB_VM_AggregateEngine_Create()}
	runtime.SetFinalizer(a, func(x *CAggregateEngine) {
		if x.ptr != nil {
			C.SVDB_VM_AggregateEngine_Destroy(x.ptr)
			x.ptr = nil
		}
	})
	return a
}

// Init resets all groups and clears state.
func (a *CAggregateEngine) Init() {
	if a.ptr != nil {
		C.SVDB_VM_AggregateEngine_Init(a.ptr)
	}
}

// Reset resets aggregate accumulators but keeps group keys.
func (a *CAggregateEngine) Reset() {
	if a.ptr != nil {
		C.SVDB_VM_AggregateEngine_Reset(a.ptr)
	}
}

// SetGroupBy selects the current group key for subsequent Accumulate calls.
func (a *CAggregateEngine) SetGroupBy(key string) {
	if a.ptr == nil {
		return
	}
	var keyPtr *C.char
	if len(key) > 0 {
		keyPtr = C.CString(key)
		defer C.free(unsafe.Pointer(keyPtr))
	}
	C.SVDB_VM_AggregateEngine_SetGroupBy(a.ptr, keyPtr, C.size_t(len(key)))
}

// AccumulateInt adds an integer value to the current group's aggregate.
func (a *CAggregateEngine) AccumulateInt(fn int, val int64) {
	if a.ptr != nil {
		C.SVDB_VM_AggregateEngine_AccumulateInt(a.ptr, C.int(fn), C.int64_t(val))
	}
}

// AccumulateFloat adds a float value to the current group's aggregate.
func (a *CAggregateEngine) AccumulateFloat(fn int, val float64) {
	if a.ptr != nil {
		C.SVDB_VM_AggregateEngine_AccumulateFloat(a.ptr, C.int(fn), C.double(val))
	}
}

// AccumulateText adds a text value to the current group's aggregate.
func (a *CAggregateEngine) AccumulateText(fn int, val string) {
	if a.ptr == nil {
		return
	}
	cs := C.CString(val)
	defer C.free(unsafe.Pointer(cs))
	C.SVDB_VM_AggregateEngine_AccumulateText(a.ptr, C.int(fn), cs, C.size_t(len(val)))
}

// Count returns the COUNT aggregate for the given group key.
func (a *CAggregateEngine) Count(groupKey string) int64 {
	if a.ptr == nil {
		return 0
	}
	cs := C.CString(groupKey)
	defer C.free(unsafe.Pointer(cs))
	return int64(C.SVDB_VM_AggregateEngine_GetCount(a.ptr, cs))
}

// SumInt returns the integer SUM aggregate for the given group key.
func (a *CAggregateEngine) SumInt(groupKey string) int64 {
	if a.ptr == nil {
		return 0
	}
	cs := C.CString(groupKey)
	defer C.free(unsafe.Pointer(cs))
	return int64(C.SVDB_VM_AggregateEngine_GetSumInt(a.ptr, cs))
}

// SumFloat returns the float SUM aggregate for the given group key.
func (a *CAggregateEngine) SumFloat(groupKey string) float64 {
	if a.ptr == nil {
		return 0
	}
	cs := C.CString(groupKey)
	defer C.free(unsafe.Pointer(cs))
	return float64(C.SVDB_VM_AggregateEngine_GetSumFloat(a.ptr, cs))
}

// Avg returns the AVG aggregate for the given group key.
func (a *CAggregateEngine) Avg(groupKey string) float64 {
	if a.ptr == nil {
		return 0
	}
	cs := C.CString(groupKey)
	defer C.free(unsafe.Pointer(cs))
	return float64(C.SVDB_VM_AggregateEngine_GetAvg(a.ptr, cs))
}

// Min returns the MIN aggregate for the given group key.
func (a *CAggregateEngine) Min(groupKey string) float64 {
	if a.ptr == nil {
		return 0
	}
	cs := C.CString(groupKey)
	defer C.free(unsafe.Pointer(cs))
	return float64(C.SVDB_VM_AggregateEngine_GetMin(a.ptr, cs))
}

// Max returns the MAX aggregate for the given group key.
func (a *CAggregateEngine) Max(groupKey string) float64 {
	if a.ptr == nil {
		return 0
	}
	cs := C.CString(groupKey)
	defer C.free(unsafe.Pointer(cs))
	return float64(C.SVDB_VM_AggregateEngine_GetMax(a.ptr, cs))
}
