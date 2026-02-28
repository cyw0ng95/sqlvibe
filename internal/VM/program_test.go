package VM

import (
	"errors"
	"testing"
)

func TestProgram_New(t *testing.T) {
	p := NewProgram()
	if p == nil {
		t.Error("NewProgram should not return nil")
	}
	if p.NumRegs != 0 {
		t.Errorf("NumRegs should be 0, got %d", p.NumRegs)
	}
	if p.NumCursors != 0 {
		t.Errorf("NumCursors should be 0, got %d", p.NumCursors)
	}
	if len(p.Instructions) != 0 {
		t.Error("Instructions should be empty")
	}
}

func TestProgram_AddInstruction(t *testing.T) {
	p := NewProgram()

	p.AddInstruction(Instruction{Op: OpLoadConst})
	p.AddInstruction(Instruction{Op: OpHalt})

	if len(p.Instructions) != 2 {
		t.Errorf("Should have 2 instructions, got %d", len(p.Instructions))
	}
	if p.Instructions[0].Op != OpLoadConst {
		t.Errorf("First instruction should be OpLoadConst, got %v", p.Instructions[0].Op)
	}
}

func TestProgram_Emit(t *testing.T) {
	p := NewProgram()

	idx := p.Emit(OpNoop)
	if idx != 0 {
		t.Errorf("First emit should return 0, got %d", idx)
	}

	idx = p.Emit(OpHalt)
	if idx != 1 {
		t.Errorf("Second emit should return 1, got %d", idx)
	}
}

func TestProgram_EmitOp(t *testing.T) {
	p := NewProgram()

	idx := p.EmitOp(OpLoadConst, 5, 10)
	if idx != 0 {
		t.Errorf("EmitOp should return 0, got %d", idx)
	}

	if p.NumRegs != 11 {
		t.Errorf("NumRegs should be 11, got %d", p.NumRegs)
	}
}

func TestProgram_EmitOpWithDst(t *testing.T) {
	p := NewProgram()

	idx := p.EmitOpWithDst(OpAdd, 1, 2, 10)
	if idx != 0 {
		t.Errorf("EmitOpWithDst should return 0, got %d", idx)
	}

	if p.NumRegs != 11 {
		t.Errorf("NumRegs should be 11, got %d", p.NumRegs)
	}
}

func TestProgram_EmitLoadConst(t *testing.T) {
	p := NewProgram()

	idx := p.EmitLoadConst(5, "test")
	if idx != 0 {
		t.Errorf("EmitLoadConst should return 0, got %d", idx)
	}

	if p.Instructions[0].Op != OpLoadConst {
		t.Errorf("Instruction should be OpLoadConst, got %v", p.Instructions[0].Op)
	}
	if p.Instructions[0].P1 != 5 {
		t.Errorf("P1 should be 5, got %d", p.Instructions[0].P1)
	}
	if p.Instructions[0].P4 != "test" {
		t.Errorf("P4 should be 'test', got %v", p.Instructions[0].P4)
	}
}

func TestProgram_EmitMove(t *testing.T) {
	p := NewProgram()

	p.EmitMove(1, 2)

	if p.NumRegs != 3 {
		t.Errorf("NumRegs should be 3, got %d", p.NumRegs)
	}
}

func TestProgram_EmitCopy(t *testing.T) {
	p := NewProgram()

	p.EmitCopy(1, 2)

	if p.NumRegs != 3 {
		t.Errorf("NumRegs should be 3, got %d", p.NumRegs)
	}
}

func TestProgram_EmitGoto(t *testing.T) {
	p := NewProgram()

	idx := p.EmitGoto(10)
	if idx != 0 {
		t.Errorf("EmitGoto should return 0, got %d", idx)
	}

	if p.Instructions[0].Op != OpGoto {
		t.Errorf("Instruction should be OpGoto, got %v", p.Instructions[0].Op)
	}
	if p.Instructions[0].P2 != 10 {
		t.Errorf("P2 should be 10, got %d", p.Instructions[0].P2)
	}
}

func TestProgram_EmitGosub(t *testing.T) {
	p := NewProgram()

	idx := p.EmitGosub(5)
	if idx != 0 {
		t.Errorf("EmitGosub should return 0, got %d", idx)
	}

	if p.Instructions[0].Op != OpGosub {
		t.Errorf("Instruction should be OpGosub, got %v", p.Instructions[0].Op)
	}
}

func TestProgram_EmitReturn(t *testing.T) {
	p := NewProgram()

	idx := p.EmitReturn()
	if idx != 0 {
		t.Errorf("EmitReturn should return 0, got %d", idx)
	}

	if p.Instructions[0].Op != OpReturn {
		t.Errorf("Instruction should be OpReturn, got %v", p.Instructions[0].Op)
	}
}

func TestProgram_EmitComparison(t *testing.T) {
	p := NewProgram()

	p.EmitEq(1, 2, 10)
	p.EmitNe(1, 2, 10)
	p.EmitLt(1, 2, 10)
	p.EmitLe(1, 2, 10)
	p.EmitGt(1, 2, 10)
	p.EmitGe(1, 2, 10)

	if len(p.Instructions) != 6 {
		t.Errorf("Should have 6 instructions, got %d", len(p.Instructions))
	}

	if p.Instructions[0].Op != OpEq {
		t.Errorf("First instruction should be OpEq, got %v", p.Instructions[0].Op)
	}
}

func TestProgram_EmitIsNull(t *testing.T) {
	p := NewProgram()

	p.EmitIsNull(5, 10)

	if p.NumRegs != 6 {
		t.Errorf("NumRegs should be 6, got %d", p.NumRegs)
	}
}

func TestProgram_EmitNotNull(t *testing.T) {
	p := NewProgram()

	p.EmitNotNull(5, 10)

	if p.NumRegs != 6 {
		t.Errorf("NumRegs should be 6, got %d", p.NumRegs)
	}
}

func TestProgram_EmitArithmetic(t *testing.T) {
	p := NewProgram()

	p.EmitAdd(3, 1, 2)
	p.EmitSubtract(3, 1, 2)
	p.EmitMultiply(3, 1, 2)
	p.EmitDivide(3, 1, 2)
	p.EmitConcat(3, 1, 2)

	if len(p.Instructions) != 5 {
		t.Errorf("Should have 5 instructions, got %d", len(p.Instructions))
	}

	if p.Instructions[0].Op != OpAdd {
		t.Errorf("First instruction should be OpAdd, got %v", p.Instructions[0].Op)
	}

	if p.NumRegs != 4 {
		t.Errorf("NumRegs should be 4, got %d", p.NumRegs)
	}
}

func TestProgram_EmitColumn(t *testing.T) {
	p := NewProgram()

	p.EmitColumn(5, 2, 3)

	if p.NumRegs != 6 {
		t.Errorf("NumRegs should be 6, got %d", p.NumRegs)
	}
	if p.NumCursors != 3 {
		t.Errorf("NumCursors should be 3, got %d", p.NumCursors)
	}
}

func TestProgram_EmitColumnWithTable(t *testing.T) {
	p := NewProgram()

	p.EmitColumnWithTable(5, 2, 3, "table1")

	if p.Instructions[0].P3 != "table1" {
		t.Errorf("P3 should be 'table1', got %v", p.Instructions[0].P3)
	}
}

func TestProgram_EmitResultRow(t *testing.T) {
	p := NewProgram()

	regs := []int{1, 2, 3}
	p.EmitResultRow(regs)

	if p.Instructions[0].Op != OpResultRow {
		t.Errorf("Instruction should be OpResultRow, got %v", p.Instructions[0].Op)
	}
}

func TestProgram_EmitHalt(t *testing.T) {
	p := NewProgram()

	err := errors.New("test error")
	p.EmitHalt(err)

	if p.Instructions[0].Op != OpHalt {
		t.Errorf("Instruction should be OpHalt, got %v", p.Instructions[0].Op)
	}
}

func TestProgram_EmitOpenTable(t *testing.T) {
	p := NewProgram()

	p.EmitOpenTable(2, "test_table")

	if p.NumCursors != 3 {
		t.Errorf("NumCursors should be 3, got %d", p.NumCursors)
	}
}

func TestProgram_EmitRewind(t *testing.T) {
	p := NewProgram()

	p.EmitRewind(0, 10)

	if p.Instructions[0].Op != OpRewind {
		t.Errorf("Instruction should be OpRewind, got %v", p.Instructions[0].Op)
	}
}

func TestProgram_EmitNext(t *testing.T) {
	p := NewProgram()

	p.EmitNext(0, 10)

	if p.Instructions[0].Op != OpNext {
		t.Errorf("Instruction should be OpNext, got %v", p.Instructions[0].Op)
	}
}

func TestProgram_Fixup(t *testing.T) {
	p := NewProgram()

	p.EmitGoto(0)
	p.Emit(OpNoop)
	p.Emit(OpNoop)
	p.Emit(OpNoop)

	p.Fixup(0)

	// Fixup sets P2 to current instruction count (which is 4 after 3 Noops)
	if p.Instructions[0].P2 != 4 {
		t.Errorf("P2 should be 4 after fixup, got %d", p.Instructions[0].P2)
	}
}

func TestProgram_FixupWithPos(t *testing.T) {
	p := NewProgram()

	p.EmitGoto(0)
	p.FixupWithPos(0, 5)

	if p.Instructions[0].P2 != 5 {
		t.Errorf("P2 should be 5, got %d", p.Instructions[0].P2)
	}
}

func TestProgram_GetInstruction(t *testing.T) {
	p := NewProgram()

	p.Emit(OpLoadConst)
	p.Emit(OpHalt)

	inst := p.GetInstruction(1)
	if inst.Op != OpHalt {
		t.Errorf("Instruction at index 1 should be OpHalt, got %v", inst.Op)
	}
}

func TestProgram_GetInstruction_OutOfBounds(t *testing.T) {
	p := NewProgram()

	inst := p.GetInstruction(10)
	if inst.Op != OpNoop {
		t.Errorf("Out of bounds should return OpNoop, got %v", inst.Op)
	}
}

func TestProgram_MarkFixup(t *testing.T) {
	p := NewProgram()

	p.MarkFixup(0)
	p.MarkFixupP2(1)

	if len(p.whereFixups) != 2 {
		t.Errorf("Should have 2 fixups, got %d", len(p.whereFixups))
	}
}

func TestProgram_ApplyWhereFixups(t *testing.T) {
	p := NewProgram()

	p.MarkFixup(0)
	p.EmitGoto(0)
	p.Emit(OpNoop)
	p.Emit(OpNoop)
	p.Emit(OpNoop)

	p.ApplyWhereFixups()

	// ApplyWhereFixups uses target = len(p.Instructions) which is 4
	if p.Instructions[0].P4 != 4 {
		t.Errorf("P4 should be 4 after fixup, got %v", p.Instructions[0].P4)
	}
}
