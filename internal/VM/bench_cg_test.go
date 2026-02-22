package VM_test

import (
	"testing"

	CG "github.com/cyw0ng95/sqlvibe/internal/CG"
	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
)

// BenchmarkCG_CSE measures how CSE (Common Subexpression Elimination) speeds
// up programs with repeated arithmetic sub-expressions.
func BenchmarkCG_CSE(b *testing.B) {
	// This SQL compiles to a program that computes the same sub-expression
	// multiple times; CSE should collapse them.
	sql := "SELECT 1 + 2, 1 + 2, 1 + 2, 3 * 4, 3 * 4"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := CG.Compile(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCG_Peephole measures how peephole optimisation removes dead
// jumps (OpGoto to next instruction).
func BenchmarkCG_Peephole(b *testing.B) {
	sql := "SELECT a, b FROM t1 WHERE a > 0"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := CG.Compile(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCG_CompileSelect measures end-to-end compilation speed for a
// moderately complex SELECT with a WHERE clause and multiple projections.
func BenchmarkCG_CompileSelect(b *testing.B) {
	sql := "SELECT a + b, c * d, e - f FROM t1 WHERE a > 0 AND b < 100"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := CG.Compile(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCG_CompileComplexExpr measures compilation with nested arithmetic.
func BenchmarkCG_CompileComplexExpr(b *testing.B) {
	sql := "SELECT (a + b) * (a + b) + (c - d) / (c - d) FROM t1"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := CG.Compile(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCG_ConstFolding measures constant folding optimisation performance.
func BenchmarkCG_ConstFolding(b *testing.B) {
	sql := "SELECT 1 + 2 + 3 + 4 + 5, 10 * 20 * 30, 'hello' || ' ' || 'world'"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := CG.Compile(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCG_StrengthReduction measures strength reduction optimisation.
func BenchmarkCG_StrengthReduction(b *testing.B) {
	// x * 2 should be reduced to x + x
	program := VM.NewProgram()
	program.EmitLoadConst(0, int64(21))
	program.EmitLoadConst(1, int64(2))
	program.EmitMultiply(2, 0, 1)
	program.Emit(VM.OpHalt)

	opt := CG.NewOptimizer()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clone-free: optimise the same program repeatedly to measure pass speed.
		opt.Optimize(program)
	}
}
