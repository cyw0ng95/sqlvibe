package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM -I${SRCDIR}/../../src/core/SF
#include "vm_opcode.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import "unsafe"

// pureGoValToC converts a Go interface{} to a C svdb_value_t.
// String/blob data is heap-allocated via C.CString / C.CBytes; the caller
// must call pureFreeC on the returned value when it is no longer needed.
func pureGoValToC(v interface{}) C.svdb_value_t {
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

// pureCValToGo converts a C svdb_value_t to a Go interface{}.
// Strings/blobs are deep-copied into Go memory.
func pureCValToGo(cv C.svdb_value_t) interface{} {
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

// pureFreeC releases any heap memory allocated inside a C svdb_value_t
// that was created by pureGoValToC.
func pureFreeC(cv C.svdb_value_t) {
if (cv.val_type == C.SVDB_VAL_TEXT || cv.val_type == C.SVDB_VAL_BLOB) && cv.str_data != nil {
C.free(unsafe.Pointer(cv.str_data))
}
}

// vmGetDst returns the destination register index for the given instruction.
// Returns -1 if no destination is found.
func vmGetDst(inst Instruction) int {
if inst.HasDst {
return inst.DstReg
}
if dst, ok := inst.P4.(int); ok {
return dst
}
return -1
}

// execPureOpcode attempts to execute inst via the C++ pure-opcode dispatcher.
// Returns true if the opcode was handled; false if it should fall through to
// the Go switch statement.
// When true is returned, the result (if any) has already been written to
// vm.registers[dst].
func (vm *VM) execPureOpcode(inst Instruction) bool {
// ── Inline trivial ops (no C++ call needed) ──────────────────────────
switch inst.Op {
case OpNull, OpConstNull:
vm.registers[inst.P1] = nil
return true

case OpMove:
vm.registers[inst.P2] = vm.registers[inst.P1]
return true
case OpCopy:
if inst.P1 != int32(inst.P2) {
vm.registers[inst.P2] = vm.registers[inst.P1]
}
return true
case OpSCopy:
vm.registers[inst.P2] = vm.registers[inst.P1]
return true
case OpIntCopy:
if v, ok := vm.registers[inst.P1].(int64); ok {
vm.registers[inst.P2] = v
} else if v, ok := vm.registers[inst.P1].(float64); ok {
vm.registers[inst.P2] = int64(v)
}
return true

case OpIfNull2:
// COALESCE-style: return v1 if non-NULL, else v2.
src := vm.registers[inst.P1]
fallback := vm.registers[inst.P2]
if dst := vmGetDst(inst); dst >= 0 {
if src == nil {
vm.registers[dst] = fallback
} else {
vm.registers[dst] = src
}
}
return true
}
// OpIsNull, OpNotNull: conditional jumps using P2 as target — stay in Go.
// OpGoto, OpGosub, OpReturn, OpInit, OpHalt, OpNoop, OpIf, OpIfNot:
//   control flow — stay in Go.

// ── Route pure computation opcodes through C++ ────────────────────────
op := int32(inst.Op)
var isCPP bool
switch inst.Op {
case OpAdd, OpSubtract, OpMultiply, OpDivide, OpRemainder, OpMod,
OpAddImm, OpBitAnd, OpBitOr, OpShiftLeft, OpShiftRight,
OpEq, OpNe, OpLt, OpLe, OpGt, OpGe, OpIs, OpIsNot,
OpConcat, OpLength, OpUpper, OpLower,
OpTrim, OpLTrim, OpRTrim, OpInstr, OpLike, OpNotLike, OpGlob, OpMatch,
OpAbs, OpRound, OpCeil, OpCeiling, OpFloor, OpSqrt, OpPow, OpExp,
OpLog, OpLog10, OpLn, OpSin, OpCos, OpTan, OpAsin, OpAcos,
OpAtan, OpAtan2, OpSinh, OpCosh, OpTanh, OpDegToRad, OpRadToDeg,
OpToText, OpToNumeric, OpToInt, OpToReal, OpRealToInt,
OpTypeof:
isCPP = true
}
if !isCPP {
return false
}

// Convert input register values to C.
var cv1, cv2 C.svdb_value_t
var pv1, pv2 *C.svdb_value_t

// First operand is always registers[P1].
cv1 = pureGoValToC(vm.registers[inst.P1])
pv1 = &cv1
defer pureFreeC(cv1)

// Second operand is registers[P2] for binary ops.
var hasP2 bool
switch inst.Op {
case OpAdd, OpSubtract, OpMultiply, OpDivide, OpRemainder, OpMod,
OpBitAnd, OpBitOr, OpShiftLeft, OpShiftRight,
OpEq, OpNe, OpLt, OpLe, OpGt, OpGe, OpIs, OpIsNot,
OpConcat, OpInstr, OpLike, OpNotLike, OpGlob, OpMatch,
OpPow, OpAtan2:
hasP2 = true
case OpAddImm:
// aux_i carries the immediate; P2 is the immediate value, not a register.
case OpRound:
// P2 holds the decimal-count register when P2 != 0.
hasP2 = false
case OpTrim, OpLTrim, OpRTrim:
// P2 = custom character set register (0 means trim spaces).
hasP2 = (inst.P2 != 0)
}
if hasP2 {
cv2 = pureGoValToC(vm.registers[inst.P2])
pv2 = &cv2
defer pureFreeC(cv2)
}

// aux_i: auxiliary integer parameter.
var auxI C.int64_t
switch inst.Op {
case OpAddImm:
auxI = C.int64_t(inst.P2)
case OpRound:
if inst.P2 != 0 {
if dv, ok := vm.registers[inst.P2].(int64); ok {
auxI = C.int64_t(dv)
} else if dv, ok := vm.registers[inst.P2].(float64); ok {
auxI = C.int64_t(int64(dv))
}
}
}

var outVal C.svdb_value_t
rc := C.svdb_vm_dispatch_pure(
C.int32_t(op),
pv1, pv2, nil,
auxI,
nil, 0,
&outVal,
)

if rc == C.SVDB_PURE_NOT_PURE || rc == C.SVDB_PURE_ERROR {
return false
}

// Convert result to Go.
result := pureCValToGo(outVal)
C.svdb_pure_value_free(&outVal)

// Write result to the appropriate destination register.
switch inst.Op {
case OpEq, OpNe, OpLt, OpLe, OpGt, OpGe, OpIs, OpIsNot:
	// P4 distinguishes two uses that share the same instruction encoding:
	//   • P4 < NumRegs  → P4 is a destination register (store 0/1/nil).
	//   • P4 >= NumRegs → P4 is a jump-target PC (jump when result is truthy).
	// This mirrors the identical branch in the Go switch (exec.go).
	if inst.P4 != nil {
		if dst, ok := inst.P4.(int); ok {
			if dst < vm.program.NumRegs {
				vm.registers[dst] = result
			} else if result != nil {
				if rv, ok2 := result.(int64); ok2 && rv != 0 {
					vm.pc = dst
				}
			}
		}
	}

case OpAdd, OpSubtract, OpMultiply, OpDivide, OpRemainder, OpMod,
OpBitAnd, OpBitOr, OpShiftLeft, OpShiftRight,
OpConcat, OpPow, OpAtan2:
// Binary ops: dest is DstReg or P4.(int).
if inst.HasDst {
vm.registers[inst.DstReg] = result
} else if dst, ok := inst.P4.(int); ok {
vm.registers[dst] = result
}

case OpAddImm:
// Result goes back into P1.
vm.registers[inst.P1] = result

default:
// Unary ops (Length, Upper, Lower, Trim*, math, type conversion, etc.):
// destination is P4.(int).
if dst, ok := inst.P4.(int); ok {
vm.registers[dst] = result
}
}

return true
}
