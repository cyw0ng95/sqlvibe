package CG_test

import (
	"testing"

	CG "github.com/cyw0ng95/sqlvibe/internal/CG"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

// BenchmarkCGO_OptimizeBytecodeInstrs_Small benchmarks optimization of a small program.
func BenchmarkCGO_OptimizeBytecodeInstrs_Small(b *testing.B) {
	// Create a small bytecode program
	bld := VM.NewBytecodeBuilder()
	bld.SetColNames([]string{"result"})
	r0 := int(bld.AllocReg())
	c0 := int(bld.AddConst(VM.VmInt(1)))
	bld.EmitABC(VM.BcLoadConst, 0, int32(c0), int32(r0))
	bld.EmitAB(VM.BcResultRow, int32(r0), 1)
	bld.Emit(VM.BcHalt)
	prog := bld.Build()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CG.OptimizeBytecodeInstrs(prog.Instrs)
	}
}

// BenchmarkCGO_OptimizeBytecodeInstrs_Medium benchmarks optimization of a medium program.
func BenchmarkCGO_OptimizeBytecodeInstrs_Medium(b *testing.B) {
	// Create a medium bytecode program with some dead code
	bld := VM.NewBytecodeBuilder()
	bld.SetColNames([]string{"a", "b", "c"})
	r0 := int(bld.AllocReg())
	r1 := int(bld.AllocReg())
	r2 := int(bld.AllocReg())
	
	c1 := int(bld.AddConst(VM.VmInt(1)))
	c2 := int(bld.AddConst(VM.VmInt(2)))
	
	bld.EmitABC(VM.BcLoadConst, 0, int32(c1), int32(r0))
	bld.EmitABC(VM.BcLoadConst, 0, int32(c2), int32(r1))
	// Dead code
	bld.EmitABC(VM.BcAdd, int32(r0), int32(r1), int32(r2))
	bld.EmitABC(VM.BcLoadConst, 0, int32(c1), int32(r0))
	bld.EmitAB(VM.BcResultRow, int32(r0), 1)
	bld.Emit(VM.BcHalt)
	prog := bld.Build()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CG.OptimizeBytecodeInstrs(prog.Instrs)
	}
}

// BenchmarkCGO_OptimizeBytecodeInstrs_Large benchmarks optimization of a large program.
func BenchmarkCGO_OptimizeBytecodeInstrs_Large(b *testing.B) {
	// Create a larger bytecode program
	bld := VM.NewBytecodeBuilder()
	bld.SetColNames([]string{"col1", "col2", "col3", "col4", "col5"})
	
	regs := make([]int32, 10)
	for i := range regs {
		regs[i] = bld.AllocReg()
	}
	
	consts := make([]int32, 10)
	for i := range consts {
		consts[i] = bld.AddConst(VM.VmInt(int64(i + 1)))
	}
	
	// Load constants
	for i := 0; i < 10; i++ {
		bld.EmitABC(VM.BcLoadConst, 0, consts[i], regs[i])
	}
	
	// Some arithmetic
	bld.EmitABC(VM.BcAdd, regs[0], regs[1], regs[5])
	bld.EmitABC(VM.BcAdd, regs[2], regs[3], regs[6])
	bld.EmitABC(VM.BcAdd, regs[5], regs[6], regs[7])
	
	// Result
	bld.EmitAB(VM.BcResultRow, regs[7], 1)
	bld.Emit(VM.BcHalt)
	prog := bld.Build()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CG.OptimizeBytecodeInstrs(prog.Instrs)
	}
}

// BenchmarkCGO_CGOptimizeProgram benchmarks the full program optimization path.
func BenchmarkCGO_CGOptimizeProgram(b *testing.B) {
	prog := VM.NewProgram()
	prog.EmitLoadConst(0, int64(42))
	prog.EmitLoadConst(1, int64(10))
	prog.EmitAdd(0, 1, 2)
	prog.EmitResultRow([]int{2})
	prog.Emit(VM.OpHalt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CG.CGOptimizeProgram(prog, []string{"result"})
	}
}

// BenchmarkCGO_PlanCache_Put benchmarks putting programs in the cache.
func BenchmarkCGO_PlanCache_Put(b *testing.B) {
	CG.CGClearPlanCache()
	
	prog := VM.NewProgram()
	prog.EmitLoadConst(0, int64(1))
	prog.EmitResultRow([]int{0})
	prog.Emit(VM.OpHalt)

	sql := "SELECT 1"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CG.CGPutPlan(sql, prog, []string{"v"})
	}
}

// BenchmarkCGO_PlanCache_Get benchmarks getting programs from the cache.
func BenchmarkCGO_PlanCache_Get(b *testing.B) {
	CG.CGClearPlanCache()
	
	prog := VM.NewProgram()
	prog.EmitLoadConst(0, int64(1))
	prog.EmitResultRow([]int{0})
	prog.Emit(VM.OpHalt)

	sql := "SELECT 1"
	CG.CGPutPlan(sql, prog, []string{"v"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CG.CGGetPlan(sql)
	}
}

// BenchmarkCGO_PlanCache_RoundTrip benchmarks put + get round trip.
func BenchmarkCGO_PlanCache_RoundTrip(b *testing.B) {
	CG.CGClearPlanCache()
	
	prog := VM.NewProgram()
	prog.EmitLoadConst(0, int64(1))
	prog.EmitResultRow([]int{0})
	prog.Emit(VM.OpHalt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sql := "SELECT " + string(rune('0'+i%10))
		CG.CGPutPlan(sql, prog, []string{"v"})
		CG.CGGetPlan(sql)
	}
}
