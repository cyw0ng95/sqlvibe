package VM

import (
	"testing"
)

func BenchmarkCompileSelect(b *testing.B) {
	sql := "SELECT a, b, c FROM t1 WHERE x = 1 AND y > 10"
	for i := 0; i < b.N; i++ {
		_, err := Compile(sql)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkVMExecution(b *testing.B) {
	sql := "SELECT 1 + 2, 'hello' || 'world', 10 * 5"
	program, err := Compile(sql)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := NewVM(program)
		err := vm.Run(nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkArithmeticOps(b *testing.B) {
	program := NewProgram()
	program.EmitLoadConst(0, int64(100))
	program.EmitLoadConst(1, int64(200))
	program.EmitAdd(2, 0, 1)
	program.EmitMultiply(3, 0, 1)
	program.EmitSubtract(4, 1, 0)
	program.Emit(OpHalt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := NewVM(program)
		err := vm.Run(nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCompareOps(b *testing.B) {
	program := NewProgram()
	program.EmitLoadConst(0, int64(100))
	program.EmitLoadConst(1, int64(200))
	program.EmitLt(0, 1, 0)
	program.Emit(OpHalt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := NewVM(program)
		err := vm.Run(nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStringOps(b *testing.B) {
	program := NewProgram()
	program.EmitLoadConst(0, "hello")
	program.EmitLoadConst(1, "world")
	program.EmitConcat(2, 0, 1)
	program.Emit(OpHalt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		vm := NewVM(program)
		err := vm.Run(nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}
