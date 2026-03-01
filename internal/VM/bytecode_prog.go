package VM

// BytecodeProg is a compiled bytecode program ready for execution.
type BytecodeProg struct {
	Instrs   []Instr  // instruction sequence
	Consts   []VmVal  // constant pool
	NumRegs  int      // number of registers used
	ColNames []string // result column names (len == BcResultRow width)
}

// BytecodeBuilder builds a BytecodeProg incrementally.
type BytecodeBuilder struct {
	instrs   []Instr
	consts   []VmVal
	numRegs  int
	labels   []int              // label index -> resolved PC (-1 = unresolved)
	fixups   []bcFixup          // forward-reference fixups
	colNames []string
}

type bcFixup struct {
	instrIdx int // index in instrs that has a forward-ref C field
	label    int // label index to resolve
}

// NewBytecodeBuilder creates an empty builder.
func NewBytecodeBuilder() *BytecodeBuilder {
	return &BytecodeBuilder{}
}

// SetColNames sets the result column names.
func (b *BytecodeBuilder) SetColNames(names []string) {
	b.colNames = names
}

// AllocReg allocates the next available register and returns its index.
func (b *BytecodeBuilder) AllocReg() int32 {
	r := int32(b.numRegs)
	b.numRegs++
	return r
}

// AddConst adds a value to the constant pool and returns its index.
// If an identical value already exists it is not deduplicated (keeps it simple).
func (b *BytecodeBuilder) AddConst(v VmVal) int32 {
	idx := int32(len(b.consts))
	b.consts = append(b.consts, v)
	return idx
}

// AllocLabel allocates a new label (initially unresolved).
func (b *BytecodeBuilder) AllocLabel() int {
	idx := len(b.labels)
	b.labels = append(b.labels, -1)
	return idx
}

// FixupLabel resolves a label to the current PC.
// Call this after emitting all instructions that the label should point to.
func (b *BytecodeBuilder) FixupLabel(label int) {
	b.labels[label] = len(b.instrs)
	// Patch any already-emitted fixups for this label.
	for _, f := range b.fixups {
		if f.label == label {
			b.instrs[f.instrIdx].C = int32(b.labels[label])
		}
	}
}

// Emit emits an instruction with no operands. Returns the PC of the emitted instruction.
func (b *BytecodeBuilder) Emit(op BcOpCode) int {
	pc := len(b.instrs)
	b.instrs = append(b.instrs, NewInstr(op))
	return pc
}

// EmitA emits an instruction with operand A.
func (b *BytecodeBuilder) EmitA(op BcOpCode, a int32) int {
	pc := len(b.instrs)
	b.instrs = append(b.instrs, NewInstrA(op, a))
	return pc
}

// EmitAB emits an instruction with operands A and B.
func (b *BytecodeBuilder) EmitAB(op BcOpCode, a, bVal int32) int {
	pc := len(b.instrs)
	b.instrs = append(b.instrs, NewInstrAB(op, a, bVal))
	return pc
}

// EmitABC emits an instruction with all three operands.
func (b *BytecodeBuilder) EmitABC(op BcOpCode, a, bv, c int32) int {
	pc := len(b.instrs)
	b.instrs = append(b.instrs, NewInstrABC(op, a, bv, c))
	return pc
}

// EmitJump emits a jump instruction with a forward reference to label.
// The C field will be patched when FixupLabel is called.
func (b *BytecodeBuilder) EmitJump(op BcOpCode, a int32, label int) int {
	pc := len(b.instrs)
	b.instrs = append(b.instrs, Instr{Op: uint16(op), A: a})
	if b.labels[label] >= 0 {
		// Already resolved.
		b.instrs[pc].C = int32(b.labels[label])
	} else {
		b.fixups = append(b.fixups, bcFixup{instrIdx: pc, label: label})
	}
	return pc
}

// Build finalises the program and returns it.
// Panics if any labels remain unresolved.
func (b *BytecodeBuilder) Build() *BytecodeProg {
	for i, lpc := range b.labels {
		if lpc < 0 {
			panic("BytecodeBuilder: unresolved label " + itoa(i))
		}
	}
	return &BytecodeProg{
		Instrs:   b.instrs,
		Consts:   b.consts,
		NumRegs:  b.numRegs,
		ColNames: b.colNames,
	}
}

// PC returns the current PC (index of the next instruction to be emitted).
func (b *BytecodeBuilder) PC() int {
	return len(b.instrs)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 10)
	if n < 0 {
		buf = append(buf, '-')
		n = -n
	}
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}
