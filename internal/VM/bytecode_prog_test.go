package VM

import (
	"testing"
)

func TestBytecodeBuilder_EmitAndBuild(t *testing.T) {
	b := NewBytecodeBuilder()

	// Allocate two registers
	r0 := b.AllocReg()
	r1 := b.AllocReg()
	r2 := b.AllocReg()

	// Add two constants
	c0 := b.AddConst(VmInt(10))
	c1 := b.AddConst(VmInt(32))

	// LoadConst r0 = consts[c0]
	b.EmitABC(BcLoadConst, 0, c0, r0)
	// LoadConst r1 = consts[c1]
	b.EmitABC(BcLoadConst, 0, c1, r1)
	// Add r2 = r0 + r1
	b.EmitABC(BcAdd, r0, r1, r2)
	// ResultRow r2, 1 col
	b.EmitAB(BcResultRow, r2, 1)
	// Halt
	b.Emit(BcHalt)

	prog := b.Build()
	if len(prog.Instrs) != 5 {
		t.Errorf("expected 5 instructions, got %d", len(prog.Instrs))
	}
	if len(prog.Consts) != 2 {
		t.Errorf("expected 2 constants, got %d", len(prog.Consts))
	}
	if prog.NumRegs != 3 {
		t.Errorf("expected 3 registers, got %d", prog.NumRegs)
	}
}

func TestBytecodeBuilder_LabelFixup(t *testing.T) {
	b := NewBytecodeBuilder()

	lbl := b.AllocLabel()
	// emit jump to unresolved label
	b.EmitJump(BcJump, 0, lbl)
	// emit noop
	b.Emit(BcNoop)
	// resolve label to here (PC=2)
	b.FixupLabel(lbl)

	prog := b.Build()
	// The jump instruction should have C = 2
	if prog.Instrs[0].C != 2 {
		t.Errorf("jump target = %d, want 2", prog.Instrs[0].C)
	}
}

func TestBytecodeBuilder_ConstPool(t *testing.T) {
	b := NewBytecodeBuilder()
	i0 := b.AddConst(VmInt(1))
	i1 := b.AddConst(VmText("hello"))
	i2 := b.AddConst(VmNull())
	b.Emit(BcHalt)
	prog := b.Build()

	if prog.Consts[i0].Int() != 1 {
		t.Error("const 0 should be 1")
	}
	if prog.Consts[i1].Text() != "hello" {
		t.Error("const 1 should be hello")
	}
	if !prog.Consts[i2].IsNull() {
		t.Error("const 2 should be NULL")
	}
}
