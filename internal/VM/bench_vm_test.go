package VM_test

import (
	"testing"

	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
	"github.com/cyw0ng95/sqlvibe/internal/VM/benchdata"
)

// BenchmarkVM_ArithmeticOps measures basic arithmetic instruction throughput.
func BenchmarkVM_ArithmeticOps(b *testing.B) {
	prog := benchdata.GenerateArithProgram(20)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VM.NewVM(prog)
		if err := vm.Run(nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkVM_ResultRow measures result row generation overhead (hot path).
func BenchmarkVM_ResultRow(b *testing.B) {
	const rows = 100
	const cols = 5
	prog := benchdata.GenerateResultRowProgram(rows, cols)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VM.NewVM(prog)
		vm.PreallocResultsFlat(rows, cols)
		if err := vm.Run(nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkVM_ResultRowNoPrealloc measures result row generation without pre-allocation.
func BenchmarkVM_ResultRowNoPrealloc(b *testing.B) {
	const rows = 100
	const cols = 5
	prog := benchdata.GenerateResultRowProgram(rows, cols)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VM.NewVM(prog)
		if err := vm.Run(nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkVM_RegisterPrealloc measures the VM Reset() allocation behaviour
// when the register slice is pre-allocated vs. created fresh each time.
func BenchmarkVM_RegisterPrealloc(b *testing.B) {
	prog := VM.NewProgram()
	prog.EmitLoadConst(0, int64(1))
	prog.EmitLoadConst(1, int64(2))
	prog.EmitAdd(2, 0, 1)
	prog.Emit(VM.OpHalt)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VM.NewVM(prog)
		if err := vm.Run(nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkVM_TypeAssertion measures the overhead of type assertions on P4
// for arithmetic opcodes.
func BenchmarkVM_TypeAssertion(b *testing.B) {
	prog := VM.NewProgram()
	// Chain of arithmetic ops where every P4 must be type-asserted as int.
	for i := 0; i < 50; i++ {
		prog.EmitLoadConst(0, int64(i+1))
		prog.EmitLoadConst(1, int64(i+2))
		prog.EmitAdd(2, 0, 1)
		prog.EmitMultiply(3, 0, 2)
		prog.EmitSubtract(4, 3, 1)
	}
	prog.Emit(VM.OpHalt)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VM.NewVM(prog)
		if err := vm.Run(nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkVM_StringLike measures LIKE pattern matching speed.
func BenchmarkVM_StringLike(b *testing.B) {
	prog := VM.NewProgram()
	prog.EmitLoadConst(0, "hello world this is a test string")
	prog.EmitLoadConst(1, "%world%")
	prog.EmitResultRow([]int{0, 1})
	prog.Emit(VM.OpHalt)

	// Run the LIKE benchmark using program-level operations
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VM.NewVM(prog)
		if err := vm.Run(nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkVM_CursorScan measures a full table scan via cursor iteration.
// This uses the query engine directly to set up cursor data.
func BenchmarkVM_CursorScan(b *testing.B) {
	const n = 200
	cols := []string{"id", "name", "value"}
	rows := benchdata.MakeIntTableRows(n, cols)

	// Build a program that scans cursor 0 and emits result rows.
	prog := VM.NewProgram()
	prog.EmitOpenTable(0, "t")
	rewindIdx := prog.EmitRewind(0, 0) // P2 patched below
	bodyStart := len(prog.Instructions)
	_ = bodyStart
	// Load two columns and emit result row
	prog.EmitColumn(2, 0, 0) // id
	prog.EmitColumn(3, 0, 2) // value
	prog.EmitResultRow([]int{2, 3})
	prog.EmitNext(0, bodyStart) // back-edge
	prog.Fixup(rewindIdx)       // jump past loop when empty
	prog.Emit(VM.OpHalt)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VM.NewVM(prog)
		vm.Cursors().OpenTableAtID(0, "t", rows, cols)
		if err := vm.Run(nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkVM_SubqueryCache measures subquery result caching performance.
func BenchmarkVM_SubqueryCache(b *testing.B) {
	// Build a simple scalar-constant program that acts like a cached subquery result.
	prog := VM.NewProgram()
	prog.EmitLoadConst(0, int64(42))
	prog.EmitResultRow([]int{0})
	prog.Emit(VM.OpHalt)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VM.NewVM(prog)
		if err := vm.Run(nil); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkVM_Aggregate measures aggregate accumulator throughput.
func BenchmarkVM_Aggregate(b *testing.B) {
	// Build a program simulating SUM accumulation over 100 integer values.
	prog := VM.NewProgram()
	prog.EmitLoadConst(0, int64(0)) // accumulator
	for i := 1; i <= 100; i++ {
		prog.EmitLoadConst(1, int64(i))
		prog.EmitAdd(0, 0, 1)
	}
	prog.EmitResultRow([]int{0})
	prog.Emit(VM.OpHalt)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VM.NewVM(prog)
		if err := vm.Run(nil); err != nil {
			b.Fatal(err)
		}
	}
}
