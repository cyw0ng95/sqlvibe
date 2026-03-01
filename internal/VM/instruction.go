package VM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/VM
#include "instruction.h"
*/
import "C"

type Instruction struct {
	Op     OpCode
	P1     int32
	P2     int32
	P3     string
	P4     interface{}
	DstReg int  // pre-extracted destination register (valid when HasDst is true)
	HasDst bool // true when P4 is an int destination register, cached to avoid type assertion
}

func NewInstruction(op OpCode) Instruction {
	return Instruction{Op: op}
}

func (i *Instruction) SetP1(p1 int32) *Instruction {
	i.P1 = p1
	return i
}

func (i *Instruction) SetP2(p2 int32) *Instruction {
	i.P2 = p2
	return i
}

func (i *Instruction) SetP3(p3 string) *Instruction {
	i.P3 = p3
	return i
}

func (i *Instruction) SetP4(p4 interface{}) *Instruction {
	i.P4 = p4
	return i
}

// toSvdbInstr converts an Instruction to the C svdb_instr_t type.
// IsJump and IsTerminal only inspect the opcode field (op), so zeroing the
// remaining fields (fl, a, b, c) is safe for those callers. The Go Instruction
// type does not carry a C-style flag bitmask, so HasFlag is not exposed.
func (i Instruction) toSvdbInstr() C.svdb_instr_t {
	return C.svdb_instr_t{
		op: C.uint16_t(i.Op),
		fl: 0,
		a:  C.int32_t(i.P1),
		b:  C.int32_t(i.P2),
		c:  0,
	}
}

// IsJump returns true if this instruction is a control-flow jump.
// Delegates to the C++ svdb_instr_is_jump which checks the opcode value only.
func (i Instruction) IsJump() bool {
	return C.svdb_instr_is_jump(i.toSvdbInstr()) != 0
}

// IsTerminal returns true if this instruction terminates execution (e.g. Halt).
// Delegates to the C++ svdb_instr_is_terminal which checks the opcode value only.
func (i Instruction) IsTerminal() bool {
	return C.svdb_instr_is_terminal(i.toSvdbInstr()) != 0
}
