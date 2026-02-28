package VM

import (
	"testing"
)

func TestRegisterAllocator_New(t *testing.T) {
	ra := NewRegisterAllocator(10)
	if ra == nil {
		t.Error("NewRegisterAllocator should not return nil")
	}
	if ra.MaxReg() != 10 {
		t.Errorf("MaxReg should be 10, got %d", ra.MaxReg())
	}
}

func TestRegisterAllocator_New_Zero(t *testing.T) {
	ra := NewRegisterAllocator(0)
	if ra.MaxReg() != 16 {
		t.Errorf("MaxReg should default to 16 for zero input, got %d", ra.MaxReg())
	}
}

func TestRegisterAllocator_New_Negative(t *testing.T) {
	ra := NewRegisterAllocator(-5)
	if ra.MaxReg() != 16 {
		t.Errorf("MaxReg should default to 16 for negative input, got %d", ra.MaxReg())
	}
}

func TestRegisterAllocator_Alloc(t *testing.T) {
	ra := NewRegisterAllocator(10)

	reg := ra.Alloc()
	if reg != 0 {
		t.Errorf("First alloc should return 0, got %d", reg)
	}

	reg = ra.Alloc()
	if reg != 1 {
		t.Errorf("Second alloc should return 1, got %d", reg)
	}
}

func TestRegisterAllocator_Alloc_Many(t *testing.T) {
	ra := NewRegisterAllocator(10)

	regs := ra.AllocMany(5)
	if len(regs) != 5 {
		t.Errorf("AllocMany should return 5 registers, got %d", len(regs))
	}

	for i := 0; i < 5; i++ {
		if regs[i] != i {
			t.Errorf("Register %d should be %d, got %d", i, i, regs[i])
		}
	}
}

func TestRegisterAllocator_Release(t *testing.T) {
	ra := NewRegisterAllocator(10)

	r1 := ra.Alloc()
	_ = ra.Alloc()

	ra.Release(r1)

	r3 := ra.Alloc()
	if r3 != r1 {
		t.Errorf("Released register should be reused, expected %d, got %d", r1, r3)
	}
}

func TestRegisterAllocator_Release_AfterLargeRegs(t *testing.T) {
	ra := NewRegisterAllocator(10)

	for i := 0; i < 100; i++ {
		ra.Alloc()
	}

	ra.Release(50)

	r := ra.Alloc()
	if r != 50 {
		t.Errorf("Released large register should be reused, expected 50, got %d", r)
	}
}

func TestRegisterAllocator_ReleaseMany(t *testing.T) {
	ra := NewRegisterAllocator(10)

	regs := ra.AllocMany(3)
	ra.ReleaseMany(regs)

	r := ra.Alloc()
	if r != 0 {
		t.Errorf("All registers should be released, expected 0, got %d", r)
	}
}

func TestRegisterAllocator_Reserve(t *testing.T) {
	ra := NewRegisterAllocator(10)

	ra.Reserve(5)

	r := ra.Alloc()
	if r != 0 {
		t.Errorf("First alloc should return 0, got %d", r)
	}

	r = ra.Alloc()
	if r != 1 {
		t.Errorf("Second alloc should return 1, got %d", r)
	}

	r = ra.Alloc()
	if r != 2 {
		t.Errorf("Third alloc should return 2, got %d", r)
	}

	r = ra.Alloc()
	if r != 3 {
		t.Errorf("Fourth alloc should return 3, got %d", r)
	}

	r = ra.Alloc()
	if r != 4 {
		t.Errorf("Fifth alloc should return 4, got %d", r)
	}

	r = ra.Alloc()
	if r != 6 {
		t.Errorf("Sixth alloc should skip reserved 5 and return 6, got %d", r)
	}
}

func TestRegisterAllocator_MaxReg(t *testing.T) {
	ra := NewRegisterAllocator(10)

	if ra.MaxReg() != 10 {
		t.Errorf("Initial MaxReg should be 10, got %d", ra.MaxReg())
	}

	ra.Alloc()
	ra.Alloc()

	if ra.MaxReg() != 10 {
		t.Errorf("MaxReg should still be 10, got %d", ra.MaxReg())
	}

	ra.Reserve(20)

	if ra.MaxReg() != 21 {
		t.Errorf("MaxReg should be 21 after reserving 20, got %d", ra.MaxReg())
	}
}

func TestRegisterAllocator_Reset(t *testing.T) {
	ra := NewRegisterAllocator(10)

	ra.Alloc()
	ra.Alloc()
	ra.Alloc()

	ra.Reset()

	if ra.MaxReg() != 10 {
		t.Errorf("MaxReg should be reset to 10, got %d", ra.MaxReg())
	}

	r := ra.Alloc()
	if r != 0 {
		t.Errorf("After reset, first alloc should return 0, got %d", r)
	}
}
