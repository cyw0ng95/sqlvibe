package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "expr_engine_api.h"
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// ExprOp mirrors the C++ ExprOp enum.
const (
	ExprOpAdd = 1
	ExprOpSub = 2
	ExprOpMul = 3
	ExprOpDiv = 4
	ExprOpMod = 5

	ExprOpEq = 10
	ExprOpNe = 11
	ExprOpLt = 12
	ExprOpLe = 13
	ExprOpGt = 14
	ExprOpGe = 15

	ExprOpAnd = 20
	ExprOpOr  = 21
	ExprOpNot = 22
)

// CExprEngine wraps the C++ ExprEngine for vectorized expression evaluation.
type CExprEngine struct {
	ptr unsafe.Pointer // opaque *SVDB_VM_ExprEngine
}

// NewCExprEngine creates a new C++ expression engine.
func NewCExprEngine() *CExprEngine {
	e := &CExprEngine{ptr: C.SVDB_VM_ExprEngine_Create()}
	runtime.SetFinalizer(e, func(x *CExprEngine) {
		if x.ptr != nil {
			C.SVDB_VM_ExprEngine_Destroy(x.ptr)
			x.ptr = nil
		}
	})
	return e
}

// EvalIntOp evaluates a binary integer arithmetic / comparison operation.
func (e *CExprEngine) EvalIntOp(op int, a, b int64) int64 {
	if e.ptr == nil {
		return 0
	}
	return int64(C.SVDB_VM_ExprEngine_EvalIntOp(e.ptr, C.int(op), C.int64_t(a), C.int64_t(b)))
}

// EvalFloatOp evaluates a binary float arithmetic operation.
func (e *CExprEngine) EvalFloatOp(op int, a, b float64) float64 {
	if e.ptr == nil {
		return 0
	}
	return float64(C.SVDB_VM_ExprEngine_EvalFloatOp(e.ptr, C.int(op), C.double(a), C.double(b)))
}

// EvalCompare evaluates a comparison and returns 1 (true) or 0 (false).
func (e *CExprEngine) EvalCompare(op int, a, b int64) bool {
	if e.ptr == nil {
		return false
	}
	return C.SVDB_VM_ExprEngine_EvalCompare(e.ptr, C.int(op), C.int64_t(a), C.int64_t(b)) != 0
}

// EvalLogic evaluates AND/OR/NOT logic (a, b are 0/1).
func (e *CExprEngine) EvalLogic(op int, a, b bool) bool {
	if e.ptr == nil {
		return false
	}
	ai, bi := 0, 0
	if a {
		ai = 1
	}
	if b {
		bi = 1
	}
	return C.SVDB_VM_ExprEngine_EvalLogic(e.ptr, C.int(op), C.int(ai), C.int(bi)) != 0
}
