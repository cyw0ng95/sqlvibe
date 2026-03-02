package CG

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb
#cgo CFLAGS: -I${SRCDIR}/../../src/core/CG
#include "expr_compiler.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// cExprPruneInstructions removes dead EOpLoadConst instructions from a
// flat int16 expression bytecode slice.  Returns the pruned slice.
func cExprPruneInstructions(in []int16) []int16 {
	if len(in) == 0 {
		return in
	}
	out := make([]int16, len(in))
	n := C.svdb_cg_expr_prune(
		(*C.int16_t)(unsafe.Pointer(&in[0])),
		C.size_t(len(in)),
		(*C.int16_t)(unsafe.Pointer(&out[0])),
		C.size_t(len(out)),
	)
	return out[:n]
}

// cExprOpcodeHistogram computes a frequency histogram of opcodes in a flat
// int16 expression bytecode slice.  Returns a slice indexed by opcode value.
func cExprOpcodeHistogram(in []int16) []int64 {
	const histSize = 32
	hist := make([]int64, histSize)
	if len(in) == 0 {
		return hist
	}
	C.svdb_cg_expr_opcode_histogram(
		(*C.int16_t)(unsafe.Pointer(&in[0])),
		C.size_t(len(in)),
		(*C.int64_t)(unsafe.Pointer(&hist[0])),
		C.size_t(histSize),
	)
	return hist
}
