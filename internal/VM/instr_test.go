package VM

import (
	"testing"
	"unsafe"
)

func TestInstr_Size(t *testing.T) {
	var inst Instr
	size := unsafe.Sizeof(inst)
	if size != 16 {
		t.Errorf("Instr size = %d, want 16", size)
	}
}

func TestInstr_Fields(t *testing.T) {
	inst := NewInstrABC(BcAdd, 1, 2, 3)
	if inst.Op != uint16(BcAdd) {
		t.Errorf("Op = %d, want %d", inst.Op, BcAdd)
	}
	if inst.A != 1 {
		t.Errorf("A = %d, want 1", inst.A)
	}
	if inst.B != 2 {
		t.Errorf("B = %d, want 2", inst.B)
	}
	if inst.C != 3 {
		t.Errorf("C = %d, want 3", inst.C)
	}
}

func TestInstr_Flags(t *testing.T) {
	inst := Instr{Op: uint16(BcLoadConst), Fl: InstrFlagConstB}
	if inst.Fl&InstrFlagConstB == 0 {
		t.Error("InstrFlagConstB should be set")
	}
	if inst.Fl&InstrFlagImmA != 0 {
		t.Error("InstrFlagImmA should not be set")
	}
}

func TestInstr_CacheLine(t *testing.T) {
	// 4 instructions should fit in a 64-byte cache line
	if unsafe.Sizeof(Instr{})*4 != 64 {
		t.Errorf("4 x Instr = %d bytes, want 64", unsafe.Sizeof(Instr{})*4)
	}
}
