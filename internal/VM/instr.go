package VM

// InstrFlag is a bitmask of flags carried by each instruction.
type InstrFlag = uint16

const (
	// InstrFlagImmA: A is a small integer immediate, not a register.
	InstrFlagImmA InstrFlag = 1 << 0

	// InstrFlagConstB: B is a constant-pool index, not a register.
	InstrFlagConstB InstrFlag = 1 << 1

	// InstrFlagJumpC: C is an absolute jump target (PC), not a register.
	InstrFlagJumpC InstrFlag = 1 << 2

	// InstrFlagTypedInt: both A and B registers are known-int at compile time.
	InstrFlagTypedInt InstrFlag = 1 << 3

	// InstrFlagTypedFloat: both A and B registers are known-float at compile time.
	InstrFlagTypedFloat InstrFlag = 1 << 4

	// InstrFlagNullable: result may be NULL; handler must propagate NULL.
	InstrFlagNullable InstrFlag = 1 << 5
)

// Instr is a fixed-width 16-byte bytecode instruction.
//
// Layout (amd64):
//
//	 0       2       4               8               12              16
//	 +-------+-------+---------------+---------------+---------------+
//	 | Op    | Fl    |       A       |       B       |       C       |
//	 +-------+-------+---------------+---------------+---------------+
//	  uint16  uint16     int32           int32           int32
//
// Four instructions fit in a single 64-byte L1 cache line.
type Instr struct {
	Op uint16
	Fl uint16
	A  int32
	B  int32
	C  int32
}

// NewInstr constructs an instruction with Op set and all operands zero.
func NewInstr(op BcOpCode) Instr {
	return Instr{Op: uint16(op)}
}

// NewInstrA constructs an instruction with A operand.
func NewInstrA(op BcOpCode, a int32) Instr {
	return Instr{Op: uint16(op), A: a}
}

// NewInstrAB constructs an instruction with A and B operands.
func NewInstrAB(op BcOpCode, a, b int32) Instr {
	return Instr{Op: uint16(op), A: a, B: b}
}

// NewInstrABC constructs an instruction with all three operands.
func NewInstrABC(op BcOpCode, a, b, c int32) Instr {
	return Instr{Op: uint16(op), A: a, B: b, C: c}
}
