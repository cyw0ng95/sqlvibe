package CG_test

import (
	"testing"

	CG "github.com/cyw0ng95/sqlvibe/internal/CG"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

// TestCGO_OptimizeBytecodeInstrs verifies that the C++ instruction optimiser
// can process a valid VM.Instr slice without panicking and returns a
// non-empty result.
func TestCGO_OptimizeBytecodeInstrs(t *testing.T) {
	b := VM.NewBytecodeBuilder()
	b.SetColNames([]string{"result"})
	r0 := b.AllocReg()
	c0 := b.AddConst(VM.VmInt(1))
	b.EmitABC(VM.BcLoadConst, 0, c0, r0)
	b.EmitAB(VM.BcResultRow, r0, 1)
	b.Emit(VM.BcHalt)
	prog := b.Build()

	optimised := CG.OptimizeBytecodeInstrs(prog.Instrs)
	if len(optimised) == 0 {
		t.Fatal("OptimizeBytecodeInstrs returned empty slice")
	}
	// Result should be <= original length
	if len(optimised) > len(prog.Instrs) {
		t.Errorf("optimised length %d > original %d", len(optimised), len(prog.Instrs))
	}
}

// TestCGO_OptimizeBytecodeInstrs_Empty tests empty input is handled gracefully.
func TestCGO_OptimizeBytecodeInstrs_Empty(t *testing.T) {
	result := CG.OptimizeBytecodeInstrs(nil)
	if result != nil {
		t.Errorf("expected nil for nil input, got %v", result)
	}
	result = CG.OptimizeBytecodeInstrs([]VM.Instr{})
	if len(result) != 0 {
		t.Errorf("expected empty slice for empty input, got len=%d", len(result))
	}
}

// TestCGO_CGOptimizeProgram verifies that the C++ program optimiser can
// process a VM.Program without error and returns a valid program.
func TestCGO_CGOptimizeProgram(t *testing.T) {
	prog := VM.NewProgram()
	prog.EmitLoadConst(0, int64(42))
	prog.EmitResultRow([]int{0})
	prog.Emit(VM.OpHalt)

	optimised := CG.CGOptimizeProgram(prog, []string{"v"})
	if optimised == nil {
		t.Fatal("CGOptimizeProgram returned nil")
	}
	if len(optimised.Instructions) == 0 {
		t.Fatal("optimised program has no instructions")
	}
}

// TestCGO_PlanCacheRoundTrip verifies the C++ plan cache stores and
// retrieves programs correctly.
func TestCGO_PlanCacheRoundTrip(t *testing.T) {
	CG.CGClearPlanCache()

	prog := VM.NewProgram()
	prog.EmitLoadConst(0, int64(7))
	prog.EmitResultRow([]int{0})
	prog.Emit(VM.OpHalt)

	CG.CGPutPlan("SELECT 7", prog, []string{"v"})

	got, cols := CG.CGGetPlan("SELECT 7")
	if got == nil {
		t.Fatal("CGGetPlan returned nil for cached program")
	}
	if len(got.Instructions) == 0 {
		t.Fatal("retrieved program has no instructions")
	}
	if len(cols) == 0 {
		t.Error("expected non-empty column names from cache")
	}
}

// TestCGO_PlanCacheMiss confirms that a cache miss returns nil.
func TestCGO_PlanCacheMiss(t *testing.T) {
	CG.CGClearPlanCache()
	got, _ := CG.CGGetPlan("SELECT 99999")
	if got != nil {
		t.Fatal("expected nil on cache miss")
	}
}

// TestCGO_PlanCacheSize verifies CGPlanCacheSize returns the right count.
func TestCGO_PlanCacheSize(t *testing.T) {
	CG.CGClearPlanCache()
	if sz := CG.CGPlanCacheSize(); sz != 0 {
		t.Errorf("expected size 0 after clear, got %d", sz)
	}
	prog := VM.NewProgram()
	prog.EmitLoadConst(0, int64(1))
	prog.EmitResultRow([]int{0})
	prog.Emit(VM.OpHalt)
	CG.CGPutPlan("SELECT 1", prog, []string{"v"})
	if sz := CG.CGPlanCacheSize(); sz != 1 {
		t.Errorf("expected size 1 after put, got %d", sz)
	}
}

// TestCGO_SetOptimizationLevel verifies that changing the optimisation level
// does not crash the library.
func TestCGO_SetOptimizationLevel(t *testing.T) {
	for _, level := range []int{0, 1, 2} {
		CG.CGSetOptimizationLevel(level)
	}
	// Restore default
	CG.CGSetOptimizationLevel(1)
}
