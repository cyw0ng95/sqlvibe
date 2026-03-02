package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>

void* SVDB_VM_Dispatcher_Create();
void SVDB_VM_Dispatcher_Destroy(void* disp);

void* SVDB_VM_State_Create();
void SVDB_VM_State_Destroy(void* state);
void SVDB_VM_State_SetRegister(void* state, int32_t idx, int64_t value);
int64_t SVDB_VM_State_GetRegister(void* state, int32_t idx);
void SVDB_VM_State_SetRegisterFloat(void* state, int32_t idx, double value);
double SVDB_VM_State_GetRegisterFloat(void* state, int32_t idx);
void SVDB_VM_State_SetRowCount(void* state, int64_t count);
int64_t SVDB_VM_State_GetRowCount(void* state);
void SVDB_VM_State_SetError(void* state, const char* err);
int SVDB_VM_State_HasError(void* state);
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// CVMState wraps the C++ VMState.
type CVMState struct {
	ptr unsafe.Pointer
}

// NewCVMState creates a new C++ VMState and registers a finalizer.
func NewCVMState() *CVMState {
	s := &CVMState{ptr: unsafe.Pointer(C.SVDB_VM_State_Create())}
	runtime.SetFinalizer(s, func(x *CVMState) {
		if x.ptr != nil {
			C.SVDB_VM_State_Destroy(x.ptr)
			x.ptr = nil
		}
	})
	return s
}

func (s *CVMState) SetRegister(idx int32, value int64) {
	C.SVDB_VM_State_SetRegister(s.ptr, C.int32_t(idx), C.int64_t(value))
}

func (s *CVMState) GetRegister(idx int32) int64 {
	return int64(C.SVDB_VM_State_GetRegister(s.ptr, C.int32_t(idx)))
}

func (s *CVMState) SetRegisterFloat(idx int32, v float64) {
	C.SVDB_VM_State_SetRegisterFloat(s.ptr, C.int32_t(idx), C.double(v))
}

func (s *CVMState) GetRegisterFloat(idx int32) float64 {
	return float64(C.SVDB_VM_State_GetRegisterFloat(s.ptr, C.int32_t(idx)))
}

func (s *CVMState) SetRowCount(n int64) {
	C.SVDB_VM_State_SetRowCount(s.ptr, C.int64_t(n))
}

func (s *CVMState) GetRowCount() int64 {
	return int64(C.SVDB_VM_State_GetRowCount(s.ptr))
}

func (s *CVMState) SetError(msg string) {
	cs := C.CString(msg)
	C.SVDB_VM_State_SetError(s.ptr, cs)
	C.free(unsafe.Pointer(cs))
}

func (s *CVMState) HasError() bool {
	return C.SVDB_VM_State_HasError(s.ptr) != 0
}

// CDispatcher wraps the C++ Dispatcher.
type CDispatcher struct {
	ptr unsafe.Pointer
}

// NewCDispatcher creates a new C++ Dispatcher and registers a finalizer.
func NewCDispatcher() *CDispatcher {
	d := &CDispatcher{ptr: unsafe.Pointer(C.SVDB_VM_Dispatcher_Create())}
	runtime.SetFinalizer(d, func(x *CDispatcher) {
		if x.ptr != nil {
			C.SVDB_VM_Dispatcher_Destroy(x.ptr)
			x.ptr = nil
		}
	})
	return d
}

// GetPtr returns the underlying C pointer for advanced use.
func (d *CDispatcher) GetPtr() unsafe.Pointer { return d.ptr }
