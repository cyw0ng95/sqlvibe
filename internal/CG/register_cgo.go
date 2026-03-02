package CG

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/../../src/core/CG
#include "register_api.h"
#include <stdlib.h>
#include <stdint.h>
*/
import "C"
import (
	"encoding/binary"
	"runtime"
	"unsafe"
)

// CRegisterAllocator is a CGO wrapper around the C++ RegisterAllocator.
type CRegisterAllocator struct {
	ptr unsafe.Pointer
}

// NewCRegisterAllocator creates a new C++ RegisterAllocator starting at baseReg.
func NewCRegisterAllocator(baseReg int32) *CRegisterAllocator {
	ra := &CRegisterAllocator{
		ptr: C.SVDB_CG_RegisterAllocator_Create(C.int32_t(baseReg)),
	}
	runtime.SetFinalizer(ra, func(x *CRegisterAllocator) {
		if x.ptr != nil {
			C.SVDB_CG_RegisterAllocator_Destroy(x.ptr)
			x.ptr = nil
		}
	})
	return ra
}

// Alloc allocates a new register and returns its number.
func (ra *CRegisterAllocator) Alloc() int32 {
	return int32(C.SVDB_CG_RegisterAllocator_Alloc(ra.ptr))
}

// Free releases a register back to the allocator.
func (ra *CRegisterAllocator) Free(reg int32) {
	C.SVDB_CG_RegisterAllocator_Free(ra.ptr, C.int32_t(reg))
}

// Reset resets the allocator to its initial state.
func (ra *CRegisterAllocator) Reset() {
	C.SVDB_CG_RegisterAllocator_Reset(ra.ptr)
}

// CInstrEmitter is a CGO wrapper around the C++ InstrEmitter.
type CInstrEmitter struct {
	ptr unsafe.Pointer
}

// NewCInstrEmitter creates a new C++ InstrEmitter.
func NewCInstrEmitter() *CInstrEmitter {
	ie := &CInstrEmitter{
		ptr: C.SVDB_CG_InstrEmitter_Create(),
	}
	runtime.SetFinalizer(ie, func(x *CInstrEmitter) {
		if x.ptr != nil {
			C.SVDB_CG_InstrEmitter_Destroy(x.ptr)
			x.ptr = nil
		}
	})
	return ie
}

// Emit emits an instruction with up to three operands.
func (ie *CInstrEmitter) Emit(op uint16, p1, p2, p3 int32) {
	C.SVDB_CG_InstrEmitter_Emit(ie.ptr, C.uint16_t(op), C.int32_t(p1), C.int32_t(p2), C.int32_t(p3))
}

// GetPosition returns the current instruction count.
func (ie *CInstrEmitter) GetPosition() int32 {
	return int32(C.SVDB_CG_InstrEmitter_GetPosition(ie.ptr))
}

// Fixup patches the jump target at pos to target.
func (ie *CInstrEmitter) Fixup(pos, target int32) {
	C.SVDB_CG_InstrEmitter_Fixup(ie.ptr, C.int32_t(pos), C.int32_t(target))
}

// GetCount returns the number of emitted instructions.
func (ie *CInstrEmitter) GetCount() int {
	return int(C.SVDB_CG_InstrEmitter_GetCount(ie.ptr))
}

// CInstrEmitterInstr is a raw instruction from the C++ InstrEmitter.
type CInstrEmitterInstr struct {
	Op    uint16
	Flags uint16
	P1    int32
	P2    int32
	P3    int32
}

// GetInstructions returns a slice of all emitted instructions.
func (ie *CInstrEmitter) GetInstructions() []CInstrEmitterInstr {
	count := ie.GetCount()
	if count == 0 {
		return nil
	}
	// Each instr is 16 bytes: uint16 op, uint16 flags, int32 p1, int32 p2, int32 p3
	buf := make([]byte, count*16)
	C.SVDB_CG_InstrEmitter_GetData(ie.ptr, unsafe.Pointer(&buf[0]))
	out := make([]CInstrEmitterInstr, count)
	for i := 0; i < count; i++ {
		off := i * 16
		out[i].Op = binary.LittleEndian.Uint16(buf[off:])
		out[i].Flags = binary.LittleEndian.Uint16(buf[off+2:])
		out[i].P1 = int32(binary.LittleEndian.Uint32(buf[off+4:]))
		out[i].P2 = int32(binary.LittleEndian.Uint32(buf[off+8:]))
		out[i].P3 = int32(binary.LittleEndian.Uint32(buf[off+12:]))
	}
	return out
}
