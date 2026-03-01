package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "registers.h"
#include <stdlib.h>
*/
import "C"
import (
	"github.com/cyw0ng95/sqlvibe/internal/SF/util"
)

// RegisterAllocator manages VM register allocation.
type RegisterAllocator struct {
	ptr *C.svdb_regalloc_t
}

// NewRegisterAllocator creates a new register allocator.
func NewRegisterAllocator(initialRegs int) *RegisterAllocator {
	if initialRegs <= 0 {
		initialRegs = 16
	}
	return &RegisterAllocator{
		ptr: C.svdb_regalloc_create(C.int(initialRegs)),
	}
}

// Alloc allocates a single register.
func (ra *RegisterAllocator) Alloc() int {
	return int(C.svdb_regalloc_alloc(ra.ptr))
}

// AllocMany allocates multiple consecutive registers.
func (ra *RegisterAllocator) AllocMany(count int) []int {
	util.Assert(count > 0, "count must be positive: %d", count)
	result := make([]int, count)
	cResult := make([]C.int, count)
	C.svdb_regalloc_alloc_many(ra.ptr, C.int(count), &cResult[0])
	for i := range result {
		result[i] = int(cResult[i])
	}
	return result
}

// Release releases a single register.
func (ra *RegisterAllocator) Release(reg int) {
	util.Assert(reg >= 0, "register index cannot be negative: %d", reg)
	if reg < 0 {
		return
	}
	C.svdb_regalloc_release(ra.ptr, C.int(reg))
}

// ReleaseMany releases multiple registers.
func (ra *RegisterAllocator) ReleaseMany(regs []int) {
	if len(regs) == 0 {
		return
	}
	cRegs := make([]C.int, len(regs))
	for i, r := range regs {
		cRegs[i] = C.int(r)
	}
	C.svdb_regalloc_release_many(ra.ptr, &cRegs[0], C.int(len(regs)))
}

// Reserve reserves a specific register.
func (ra *RegisterAllocator) Reserve(reg int) {
	util.Assert(reg >= 0, "register index cannot be negative: %d", reg)
	if reg < 0 {
		return
	}
	C.svdb_regalloc_reserve(ra.ptr, C.int(reg))
}

// MaxReg returns the maximum register number + 1.
func (ra *RegisterAllocator) MaxReg() int {
	return int(C.svdb_regalloc_max_reg(ra.ptr))
}

// Reset resets the allocator.
func (ra *RegisterAllocator) Reset() {
	C.svdb_regalloc_reset(ra.ptr)
}

// AllocatedCount returns the number of allocated registers.
func (ra *RegisterAllocator) AllocatedCount() int {
	return int(C.svdb_regalloc_allocated_count(ra.ptr))
}
