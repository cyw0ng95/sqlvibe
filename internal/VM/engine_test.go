package VM

import (
	"testing"
)

func TestVM_NewVM(t *testing.T) {
	program := NewProgram()
	vm := NewVM(program)
	if vm == nil {
		t.Error("NewVM should not return nil")
	}
	if vm.PC() != 0 {
		t.Errorf("PC should start at 0, got %d", vm.PC())
	}
	if vm.Program() != program {
		t.Error("Program mismatch")
	}
}

func TestVM_NewVMWithContext(t *testing.T) {
	program := NewProgram()
	mockCtx := &mockVmContext{}
	vm := NewVMWithContext(program, mockCtx)
	if vm == nil {
		t.Error("NewVMWithContext should not return nil")
	}
	if vm.ctx != mockCtx {
		t.Error("Context not set correctly")
	}
}

func TestVM_Reset(t *testing.T) {
	program := NewProgram()
	vm := NewVM(program)

	vm.SetRegister(0, "test")
	vm.SetPC(10)
	vm.SetError(ErrHalt)

	vm.Reset()

	if vm.PC() != 0 {
		t.Errorf("PC should be 0 after reset, got %d", vm.PC())
	}
	if vm.GetRegister(0) != nil {
		t.Error("Registers should be cleared after reset")
	}
	if vm.Error() != nil {
		t.Error("Error should be nil after reset")
	}
}

func TestVM_SetGetRegister(t *testing.T) {
	vm := NewVM(NewProgram())

	vm.SetRegister(0, "value1")
	vm.SetRegister(1, 42)
	vm.SetRegister(2, 3.14)

	if vm.GetRegister(0) != "value1" {
		t.Errorf("Register 0 mismatch: got %v", vm.GetRegister(0))
	}
	if vm.GetRegister(1) != 42 {
		t.Errorf("Register 1 mismatch: got %v", vm.GetRegister(1))
	}
	if vm.GetRegister(2) != 3.14 {
		t.Errorf("Register 2 mismatch: got %v", vm.GetRegister(2))
	}

	if vm.GetRegister(-1) != nil {
		t.Error("Out of bounds register should return nil")
	}
	if vm.GetRegister(10000) != nil {
		t.Error("Out of bounds register should return nil")
	}
}

func TestVM_PC(t *testing.T) {
	vm := NewVM(NewProgram())

	vm.SetPC(5)
	if vm.PC() != 5 {
		t.Errorf("PC should be 5, got %d", vm.PC())
	}
}

func TestVM_GetInstruction(t *testing.T) {
	program := NewProgram()
	program.Instructions = []Instruction{
		{Op: OpLoadConst, P1: 0, P4: "test"},
		{Op: OpNoop},
		{Op: OpHalt},
	}

	vm := NewVM(program)

	inst := vm.GetInstruction()
	if inst.Op != OpLoadConst {
		t.Errorf("First instruction should be OpLoadConst, got %v", inst.Op)
	}
	if vm.PC() != 1 {
		t.Errorf("PC should be 1, got %d", vm.PC())
	}

	inst = vm.GetInstruction()
	if inst.Op != OpNoop {
		t.Errorf("Second instruction should be OpNoop, got %v", inst.Op)
	}
}

func TestVM_GetInstruction_OutOfBounds(t *testing.T) {
	program := NewProgram()
	program.Instructions = []Instruction{}

	vm := NewVM(program)

	inst := vm.GetInstruction()
	if inst.Op != OpNoop {
		t.Errorf("Out of bounds should return Noop, got %v", inst.Op)
	}
}

func TestVM_Error(t *testing.T) {
	vm := NewVM(NewProgram())

	if vm.Error() != nil {
		t.Error("Initial error should be nil")
	}

	vm.SetError(ErrHalt)
	if vm.Error() != ErrHalt {
		t.Error("Error not set correctly")
	}

	if vm.ErrorCode() != 0 {
		t.Errorf("Error code should be 0, got %d", vm.ErrorCode())
	}
}

func TestVM_Results(t *testing.T) {
	vm := NewVM(NewProgram())

	results := vm.Results()
	if results == nil {
		t.Error("Results should not be nil")
	}
}

func TestVM_PreallocResults(t *testing.T) {
	vm := NewVM(NewProgram())

	vm.PreallocResults(100)
	if cap(vm.results) < 100 {
		t.Errorf("Results should be preallocated, got cap %d", cap(vm.results))
	}

	vm.PreallocResults(0)
	vm.PreallocResults(-1)
}

func TestVM_Cursors(t *testing.T) {
	vm := NewVM(NewProgram())

	cursors := vm.Cursors()
	if cursors == nil {
		t.Error("Cursors should not be nil")
	}
}

func TestVM_Context(t *testing.T) {
	vm := NewVM(NewProgram())
	mockCtx := &mockVmContext{}

	vm.SetContext(mockCtx)
	if vm.Context() != mockCtx {
		t.Error("Context not returned correctly")
	}
}

func TestBranchPredictor(t *testing.T) {
	bp := &BranchPredictor{}

	// After enough taken branches, prediction should be true
	for i := 0; i < 5; i++ {
		bp.Update(100, true)
	}
	if !bp.Predict(100) {
		t.Error("Prediction should be true after multiple taken branches")
	}

	// After enough not-taken branches, prediction should be false
	for i := 0; i < 5; i++ {
		bp.Update(100, false)
	}
	if bp.Predict(100) {
		t.Error("Prediction should be false after multiple not-taken branches")
	}
}

func TestBranchPredictor_Saturating(t *testing.T) {
	bp := &BranchPredictor{}

	for i := 0; i < 10; i++ {
		bp.Update(100, true)
	}
	if bp.Predict(100) != true {
		t.Error("Should stay at max value")
	}

	for i := 0; i < 10; i++ {
		bp.Update(100, false)
	}
	if bp.Predict(100) != false {
		t.Error("Should stay at min value")
	}
}

func TestBranchPredictor_Modulo(t *testing.T) {
	bp := &BranchPredictor{}

	bp.Update(0, true)
	bp.Update(1024, true)

	if bp.table[0] != bp.table[1024%1024] {
		t.Error("Modulo wraparound not working")
	}
}

type mockVmContext struct{}

func (m *mockVmContext) GetTableData(tableName string) ([]map[string]interface{}, error) {
	return nil, nil
}

func (m *mockVmContext) GetTableColumns(tableName string) ([]string, error) {
	return nil, nil
}

func (m *mockVmContext) InsertRow(tableName string, row map[string]interface{}) error {
	return nil
}

func (m *mockVmContext) UpdateRow(tableName string, rowIndex int, row map[string]interface{}) error {
	return nil
}

func (m *mockVmContext) DeleteRow(tableName string, rowIndex int) error {
	return nil
}
