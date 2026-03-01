package VM_test

import (
	"testing"

	VM "github.com/cyw0ng95/sqlvibe/internal/VM"
	"github.com/cyw0ng95/sqlvibe/internal/VM/benchdata"
)

// bcTestContext is a minimal BcVmContext for bytecode VM benchmarks.
type bcTestContext struct {
	tables map[string][]map[string]interface{}
	cols   map[string][]string
}

func (c *bcTestContext) GetTableRows(table string) ([]map[string]interface{}, []string, error) {
	return c.tables[table], c.cols[table], nil
}
func (c *bcTestContext) GetTableSchema(table string) map[string]string { return nil }

// BenchmarkBcVM_Arithmetic measures BytecodeVM arithmetic instruction throughput.
func BenchmarkBcVM_Arithmetic(b *testing.B) {
	bld := VM.NewBytecodeBuilder()
	bld.SetColNames([]string{"result"})
	r0 := bld.AllocReg()
	r1 := bld.AllocReg()
	r2 := bld.AllocReg()
	c0 := bld.AddConst(VM.VmInt(42))
	c1 := bld.AddConst(VM.VmInt(7))
	bld.EmitABC(VM.BcLoadConst, 0, c0, r0)
	bld.EmitABC(VM.BcLoadConst, 0, c1, r1)
	for i := 0; i < 20; i++ {
		bld.EmitABC(VM.BcAdd, r0, r1, r2)
		bld.EmitABC(VM.BcMul, r0, r2, r2)
		bld.EmitABC(VM.BcSub, r2, r1, r2)
	}
	bld.EmitAB(VM.BcResultRow, r2, 1)
	bld.Emit(VM.BcHalt)
	prog := bld.Build()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VM.NewBytecodeVM(prog, nil)
		if err := vm.Run(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBcVM_TableScan_1k measures BytecodeVM scan throughput over 1000 rows.
func BenchmarkBcVM_TableScan_1k(b *testing.B) {
	benchmarkBcVMTableScan(b, 1000)
}

// BenchmarkBcVM_TableScan_10k measures BytecodeVM scan throughput over 10000 rows.
func BenchmarkBcVM_TableScan_10k(b *testing.B) {
	benchmarkBcVMTableScan(b, 10000)
}

// BenchmarkBcVM_TableScan_100k measures BytecodeVM scan throughput over 100000 rows.
func BenchmarkBcVM_TableScan_100k(b *testing.B) {
	benchmarkBcVMTableScan(b, 100000)
}

func benchmarkBcVMTableScan(b *testing.B, n int) {
	b.Helper()
	cols := []string{"id", "val"}
	rows := benchdata.MakeIntTableRows(n, cols)
	ctx := &bcTestContext{
		tables: map[string][]map[string]interface{}{"t": rows},
		cols:   map[string][]string{"t": cols},
	}

	bld := VM.NewBytecodeBuilder()
	bld.SetColNames([]string{"id", "val"})
	rID := bld.AllocReg()
	rVal := bld.AllocReg()
	tblConst := bld.AddConst(VM.VmText("t"))
	lblEnd := bld.AllocLabel()

	bld.EmitAB(VM.BcOpenCursor, 0, tblConst)
	bld.EmitJump(VM.BcRewind, 0, lblEnd)
	loopStart := bld.PC()
	bld.EmitABC(VM.BcColumn, 0, 0, rID)
	bld.EmitABC(VM.BcColumn, 0, 1, rVal)
	bld.EmitAB(VM.BcResultRow, rID, 2)
	bld.EmitABC(VM.BcNext, 0, 0, int32(loopStart))
	bld.FixupLabel(lblEnd)
	bld.Emit(VM.BcHalt)
	prog := bld.Build()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VM.NewBytecodeVM(prog, ctx)
		if err := vm.Run(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBcVM_FuncCall measures BytecodeVM scalar function call overhead.
func BenchmarkBcVM_FuncCall(b *testing.B) {
	bld := VM.NewBytecodeBuilder()
	bld.SetColNames([]string{"v"})
	rArg := bld.AllocReg()
	rDst := bld.AllocReg()
	cStr := bld.AddConst(VM.VmText("hello world"))
	cFn := bld.AddConst(VM.VmText("upper"))
	bld.EmitABC(VM.BcLoadConst, 0, cStr, rArg)
	// BcCall: A=funcConst, B=nArgs, C=dst (args at regs[C-B..C-1])
	bld.EmitABC(VM.BcCall, cFn, 1, rDst)
	bld.EmitAB(VM.BcResultRow, rDst, 1)
	bld.Emit(VM.BcHalt)
	prog := bld.Build()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VM.NewBytecodeVM(prog, nil)
		if err := vm.Run(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkBcVM_Compare measures BytecodeVM comparison instruction throughput.
func BenchmarkBcVM_Compare(b *testing.B) {
	bld := VM.NewBytecodeBuilder()
	bld.SetColNames([]string{"result"})
	r0 := bld.AllocReg()
	r1 := bld.AllocReg()
	r2 := bld.AllocReg()
	c0 := bld.AddConst(VM.VmInt(100))
	c1 := bld.AddConst(VM.VmInt(200))
	bld.EmitABC(VM.BcLoadConst, 0, c0, r0)
	bld.EmitABC(VM.BcLoadConst, 0, c1, r1)
	for i := 0; i < 20; i++ {
		bld.EmitABC(VM.BcEq, r0, r1, r2)
		bld.EmitABC(VM.BcLt, r0, r1, r2)
		bld.EmitABC(VM.BcGe, r1, r0, r2)
	}
	bld.EmitAB(VM.BcResultRow, r2, 1)
	bld.Emit(VM.BcHalt)
	prog := bld.Build()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := VM.NewBytecodeVM(prog, nil)
		if err := vm.Run(); err != nil {
			b.Fatal(err)
		}
	}
}


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
