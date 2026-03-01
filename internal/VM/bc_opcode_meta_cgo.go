package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "opcodes.h"
*/
import "C"

// SvdbOpcodeName returns the human-readable name of a C++ svdb bytecode opcode.
// op should be one of the SVDB_BC_* constants (0..45).
func SvdbOpcodeName(op int) string {
	name := C.svdb_opcode_name(C.int(op))
	if name == nil {
		return ""
	}
	return C.GoString(name)
}

// SvdbOpcodeNumOperands returns the number of explicit operands for a C++ bytecode opcode.
func SvdbOpcodeNumOperands(op int) int {
	return int(C.svdb_opcode_num_operands(C.int(op)))
}

// SvdbOpcodeIsJump returns true if the C++ bytecode opcode is a control-flow jump.
func SvdbOpcodeIsJump(op int) bool {
	return C.svdb_opcode_is_jump(C.int(op)) != 0
}

// SvdbOpcodeIsLoad returns true if the C++ bytecode opcode loads a value.
func SvdbOpcodeIsLoad(op int) bool {
	return C.svdb_opcode_is_load(C.int(op)) != 0
}

// SvdbOpcodeIsArith returns true if the C++ bytecode opcode is arithmetic.
func SvdbOpcodeIsArith(op int) bool {
	return C.svdb_opcode_is_arith(C.int(op)) != 0
}

// SvdbOpcodeIsCompare returns true if the C++ bytecode opcode is a comparison.
func SvdbOpcodeIsCompare(op int) bool {
	return C.svdb_opcode_is_compare(C.int(op)) != 0
}

// SvdbOpcodeIsTerminal returns true if the C++ bytecode opcode is HALT or RESULT_ROW.
func SvdbOpcodeIsTerminal(op int) bool {
	return C.svdb_opcode_is_terminal(C.int(op)) != 0
}
