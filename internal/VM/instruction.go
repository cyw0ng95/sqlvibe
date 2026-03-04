// Package VM - minimal Instruction and VM types for C++ wrapper compatibility
package VM

// Instruction represents a VM instruction.
// This is a minimal type definition for C++ wrapper compatibility.
// The actual instruction handling is done in C++.
type Instruction struct {
	Op     uint16      // Opcode
	Fl     uint16      // Flags
	A      int32       // Operand A (alias for P1)
	B      int32       // Operand B (alias for P2)
	C      int32       // Operand C (alias for P3)
	P1     int32       // Operand P1
	P2     int32       // Operand P2
	P3     int32       // Operand P3
	P4     interface{} // Operand P4 (can be int, string, etc.)
	P5     int32       // Operand P5
	HasDst bool        // Whether instruction has destination register
	DstReg int         // Destination register index
}

// VM opcodes (minimal set for wrapper compatibility)
const (
	OpNull uint16 = iota
	OpConstNull
	OpMove
	OpCopy
	OpSCopy
	OpIntCopy
	OpIfNull
	OpIfNull2
	OpNotNull
	OpIsNull
	OpEq
	OpNe
	OpLt
	OpLe
	OpGt
	OpGe
	OpIs
	OpIsNot
	OpAdd
	OpSubtract
	OpMultiply
	OpDivide
	OpRemainder
	OpMod
	OpAddImm
	OpBitAnd
	OpBitOr
	OpShiftLeft
	OpShiftRight
	OpConcat
	OpLength
	OpUpper
	OpLower
	OpTrim
	OpLTrim
	OpRTrim
	OpInstr
	OpLike
	OpNotLike
	OpGlob
	OpMatch
	OpAbs
	OpRound
	OpCeil
	OpCeiling
	OpFloor
	OpSqrt
	OpPow
	OpExp
	OpLog
	OpLog10
	OpLn
	OpSin
	OpCos
	OpTan
	OpAsin
	OpAcos
	OpAtan
	OpAtan2
	OpSinh
	OpCosh
	OpTanh
	OpDegToRad
	OpRadToDeg
	OpToText
	OpToNumeric
	OpToInt
	OpToReal
	OpRealToInt
	OpTypeof
	OpGoto
	OpGosub
	OpReturn
	OpInit
	OpHalt
	OpNoop
	OpIf
	OpIfNot
)

// VM represents the virtual machine.
// This is a minimal type definition for C++ wrapper compatibility.
// The actual VM implementation is in C++.
type VM struct {
	registers []interface{}
	program   *Program // Program pointer
	pc        int      // Program counter
}

// Instr is an alias for Instruction.
type Instr = Instruction
