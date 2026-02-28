package VM

import (
	"errors"
	"testing"
)

func TestVM_Exec_LoadConst(t *testing.T) {
	program := NewProgram()
	program.EmitLoadConst(0, 42)
	program.EmitLoadConst(1, "hello")
	program.Emit(OpHalt)

	vm := NewVM(program)
	err := vm.Exec(nil)
	if err != nil {
		t.Errorf("Exec failed: %v", err)
	}

	if vm.GetRegister(0) != 42 {
		t.Errorf("Register 0 should be 42, got %v", vm.GetRegister(0))
	}
	if vm.GetRegister(1) != "hello" {
		t.Errorf("Register 1 should be hello, got %v", vm.GetRegister(1))
	}
}

func TestVM_Exec_Move(t *testing.T) {
	program := NewProgram()
	program.EmitLoadConst(0, 42)
	program.EmitMove(0, 1)
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	if vm.GetRegister(1) != 42 {
		t.Errorf("Register 1 should be 42, got %v", vm.GetRegister(1))
	}
}

func TestVM_Exec_Copy(t *testing.T) {
	program := NewProgram()
	program.EmitLoadConst(0, 42)
	program.EmitCopy(0, 1)
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	if vm.GetRegister(1) != 42 {
		t.Errorf("Register 1 should be 42, got %v", vm.GetRegister(1))
	}
}

func TestVM_Exec_Null(t *testing.T) {
	program := NewProgram()
	inst := program.Emit(OpNull)
	program.Instructions[inst].P1 = 0
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	if vm.GetRegister(0) != nil {
		t.Errorf("Register 0 should be nil, got %v", vm.GetRegister(0))
	}
}

func TestVM_Exec_ConstNull(t *testing.T) {
	program := NewProgram()
	inst := program.Emit(OpConstNull)
	program.Instructions[inst].P1 = 0
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	if vm.GetRegister(0) != nil {
		t.Errorf("Register 0 should be nil, got %v", vm.GetRegister(0))
	}
}

func TestVM_Exec_ResultRow(t *testing.T) {
	program := NewProgram()
	program.EmitLoadConst(0, 1)
	program.EmitLoadConst(1, "test")
	program.EmitResultRow([]int{0, 1})
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	results := vm.Results()
	if len(results) != 1 {
		t.Errorf("Expected 1 result row, got %d", len(results))
	}
	if len(results[0]) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(results[0]))
	}
}

func TestVM_Exec_ResultRowMultiple(t *testing.T) {
	program := NewProgram()
	program.EmitLoadConst(0, 1)
	program.EmitResultRow([]int{0})
	program.EmitLoadConst(0, 2)
	program.EmitResultRow([]int{0})
	program.EmitLoadConst(0, 3)
	program.EmitResultRow([]int{0})
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	results := vm.Results()
	if len(results) != 3 {
		t.Errorf("Expected 3 result rows, got %d", len(results))
	}
}

func TestVM_Exec_HaltWithError(t *testing.T) {
	program := NewProgram()
	program.EmitLoadConst(0, 1)
	program.EmitHalt(errors.New("test error"))

	vm := NewVM(program)
	err := vm.Exec(nil)

	if err == nil {
		t.Error("Expected error to be returned")
	}
}

func TestVM_Exec_HaltNoError(t *testing.T) {
	program := NewProgram()
	program.EmitLoadConst(0, 1)
	program.EmitHalt(nil)

	vm := NewVM(program)
	err := vm.Exec(nil)

	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
}

func TestVM_Exec_ResultRowValues(t *testing.T) {
	program := NewProgram()
	program.EmitLoadConst(0, 42)
	program.EmitLoadConst(1, "test")
	program.EmitLoadConst(2, 3.14)
	program.EmitResultRow([]int{0, 1, 2})
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	results := vm.Results()
	if len(results) != 1 {
		t.Fatalf("Expected 1 row, got %d", len(results))
	}

	if results[0][0] != int64(42) && results[0][0] != 42 {
		t.Errorf("First col should be 42, got %v", results[0][0])
	}
	if results[0][1] != "test" {
		t.Errorf("Second col should be test, got %v", results[0][1])
	}
	if results[0][2] != 3.14 {
		t.Errorf("Third col should be 3.14, got %v", results[0][2])
	}
}

func TestVM_Exec_StringLoad(t *testing.T) {
	program := NewProgram()
	program.EmitLoadConst(0, "hello world")
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	if vm.GetRegister(0) != "hello world" {
		t.Errorf("Register 0 should be 'hello world', got %v", vm.GetRegister(0))
	}
}

func TestVM_Exec_FloatLoad(t *testing.T) {
	program := NewProgram()
	program.EmitLoadConst(0, 3.14159)
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	if vm.GetRegister(0) != 3.14159 {
		t.Errorf("Register 0 should be 3.14159, got %v", vm.GetRegister(0))
	}
}

func TestVM_Exec_BooleanLoad(t *testing.T) {
	program := NewProgram()
	program.EmitLoadConst(0, true)
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	if vm.GetRegister(0) != true {
		t.Errorf("Register 0 should be true, got %v", vm.GetRegister(0))
	}
}

func TestVM_Exec_MultipleRegs(t *testing.T) {
	program := NewProgram()
	for i := 0; i < 10; i++ {
		program.EmitLoadConst(i, i)
	}
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	for i := 0; i < 10; i++ {
		if vm.GetRegister(i) != i {
			t.Errorf("Register %d should be %d, got %v", i, i, vm.GetRegister(i))
		}
	}
}

func TestVM_Exec_Noop(t *testing.T) {
	program := NewProgram()
	program.EmitLoadConst(0, 42)
	program.Emit(OpNoop)
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	if vm.GetRegister(0) != 42 {
		t.Errorf("Register 0 should be 42, got %v", vm.GetRegister(0))
	}
}

func TestVM_Exec_MoveDifferentRegs(t *testing.T) {
	program := NewProgram()
	program.EmitLoadConst(5, 100)
	program.EmitMove(5, 10)
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	if vm.GetRegister(10) != 100 {
		t.Errorf("Register 10 should be 100, got %v", vm.GetRegister(10))
	}
}

func TestVM_PreallocResultsFlat(t *testing.T) {
	vm := NewVM(NewProgram())
	vm.PreallocResultsFlat(100, 5)

	if cap(vm.results) < 100 {
		t.Errorf("Results capacity should be at least 100, got %d", cap(vm.results))
	}
}

func TestVM_RowsAffected(t *testing.T) {
	vm := NewVM(NewProgram())

	if vm.RowsAffected() != 0 {
		t.Errorf("Initial rows affected should be 0, got %d", vm.RowsAffected())
	}
}

func TestVM_Exec_EmptyProgram(t *testing.T) {
	program := NewProgram()
	program.Emit(OpHalt)

	vm := NewVM(program)
	err := vm.Exec(nil)

	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
}

func TestVM_Exec_LargeResultSet(t *testing.T) {
	program := NewProgram()
	for i := 0; i < 1000; i++ {
		program.EmitLoadConst(0, i)
		program.EmitResultRow([]int{0})
	}
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	results := vm.Results()
	if len(results) != 1000 {
		t.Errorf("Expected 1000 rows, got %d", len(results))
	}
}

func TestVM_SetError(t *testing.T) {
	vm := NewVM(NewProgram())
	vm.SetError(errors.New("test"))

	if vm.Error() == nil {
		t.Error("Expected error to be set")
	}
}

func TestVM_ResultsEmpty(t *testing.T) {
	program := NewProgram()
	program.Emit(OpHalt)

	vm := NewVM(program)
	vm.Exec(nil)

	results := vm.Results()
	if results == nil {
		t.Error("Results should not be nil")
	}
	if len(results) != 0 {
		t.Errorf("Expected 0 results, got %d", len(results))
	}
}

func TestVM_PCSet(t *testing.T) {
	vm := NewVM(NewProgram())
	vm.SetPC(10)

	if vm.PC() != 10 {
		t.Errorf("PC should be 10, got %d", vm.PC())
	}
}

func TestVM_ProgramAccess(t *testing.T) {
	program := NewProgram()
	vm := NewVM(program)

	if vm.Program() != program {
		t.Error("Program should match")
	}
}
