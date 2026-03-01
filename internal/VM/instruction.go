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
// P3 and P4 are Go-only fields with no C equivalent, so only Op, P1, and P2
// are mapped. The fl (flags) and c fields are zeroed intentionally: the Go
// Instruction type does not carry a C-style flag bitmask, and the P3 field
// (a Go string) has no C int32 representation. IsJump/IsTerminal operate
// purely on the opcode value, so zeroed ancillary fields are safe.
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
func (i Instruction) IsJump() bool {
	return C.svdb_instr_is_jump(i.toSvdbInstr()) != 0
}

// IsTerminal returns true if this instruction terminates execution (e.g. Halt).
func (i Instruction) IsTerminal() bool {
	return C.svdb_instr_is_terminal(i.toSvdbInstr()) != 0
}

// HasFlag returns true if the given flag bit is set on this instruction.
func (i Instruction) HasFlag(flag uint16) bool {
	return C.svdb_instr_has_flag(i.toSvdbInstr(), C.uint16_t(flag)) != 0
}
