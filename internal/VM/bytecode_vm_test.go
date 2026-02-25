package VM

import (
	"testing"
)

// testContext is a simple BcVmContext for unit tests.
type testContext struct {
	tables map[string][]map[string]interface{}
	cols   map[string][]string
}

func (tc *testContext) GetTableRows(table string) ([]map[string]interface{}, []string, error) {
	return tc.tables[table], tc.cols[table], nil
}
func (tc *testContext) GetTableSchema(table string) map[string]string { return nil }

func TestBytecodeVM_LoadConst(t *testing.T) {
	b := NewBytecodeBuilder()
	b.SetColNames([]string{"result"})
	r0 := b.AllocReg()
	c0 := b.AddConst(VmInt(42))
	b.EmitABC(BcLoadConst, 0, c0, r0)
	b.EmitAB(BcResultRow, r0, 1)
	b.Emit(BcHalt)
	prog := b.Build()

	vm := NewBytecodeVM(prog, nil)
	if err := vm.Run(); err != nil {
		t.Fatal(err)
	}
	rows := vm.ResultRows()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != int64(42) {
		t.Errorf("result = %v, want 42", rows[0][0])
	}
}

func TestBytecodeVM_Arithmetic(t *testing.T) {
	b := NewBytecodeBuilder()
	b.SetColNames([]string{"sum"})
	r0 := b.AllocReg()
	r1 := b.AllocReg()
	r2 := b.AllocReg()
	c0 := b.AddConst(VmInt(10))
	c1 := b.AddConst(VmInt(32))
	b.EmitABC(BcLoadConst, 0, c0, r0)
	b.EmitABC(BcLoadConst, 0, c1, r1)
	b.EmitABC(BcAdd, r0, r1, r2)
	b.EmitAB(BcResultRow, r2, 1)
	b.Emit(BcHalt)
	prog := b.Build()

	vm := NewBytecodeVM(prog, nil)
	if err := vm.Run(); err != nil {
		t.Fatal(err)
	}
	rows := vm.ResultRows()
	if len(rows) != 1 || rows[0][0] != int64(42) {
		t.Errorf("10+32 = %v, want 42", rows[0][0])
	}
}

func TestBytecodeVM_Jump(t *testing.T) {
	// Load 0, jump over load-99, emit result — should get 0.
	b := NewBytecodeBuilder()
	b.SetColNames([]string{"v"})
	r0 := b.AllocReg()
	c0 := b.AddConst(VmInt(0))
	c1 := b.AddConst(VmInt(99))
	lbl := b.AllocLabel()

	b.EmitABC(BcLoadConst, 0, c0, r0)
	b.EmitJump(BcJump, 0, lbl)
	b.EmitABC(BcLoadConst, 0, c1, r0) // skipped
	b.FixupLabel(lbl)
	b.EmitAB(BcResultRow, r0, 1)
	b.Emit(BcHalt)

	prog := b.Build()
	vm := NewBytecodeVM(prog, nil)
	if err := vm.Run(); err != nil {
		t.Fatal(err)
	}
	rows := vm.ResultRows()
	if len(rows) != 1 || rows[0][0] != int64(0) {
		t.Errorf("expected 0 after jump, got %v", rows[0][0])
	}
}

// TestBytecodeVM_TableScan scans a 3-row table collecting all n values.
//
// Loop pattern (SQLite-style):
//
//	OpenCursor 0, "nums"
//	Rewind 0 → lblEnd  (jump lblEnd if empty)
//	loopStart:
//	  Column 0, 0 → r0
//	  ResultRow r0, 1
//	  Next 0 → loopStart  (jump loopStart while rows remain; fall-through when done)
//	lblEnd:
//	Halt
func TestBytecodeVM_TableScan(t *testing.T) {
	ctx := &testContext{
		tables: map[string][]map[string]interface{}{
			"nums": {
				{"n": int64(1)},
				{"n": int64(2)},
				{"n": int64(3)},
			},
		},
		cols: map[string][]string{"nums": {"n"}},
	}

	b := NewBytecodeBuilder()
	b.SetColNames([]string{"n"})
	r0 := b.AllocReg()
	tblName := b.AddConst(VmText("nums"))
	lblEnd := b.AllocLabel()

	b.EmitAB(BcOpenCursor, 0, tblName)
	b.EmitJump(BcRewind, 0, lblEnd) // jump lblEnd if empty

	loopStart := b.PC()
	b.EmitABC(BcColumn, 0, 0, r0)
	b.EmitAB(BcResultRow, r0, 1)
	// BcNext: A=cursor, C=loopStart; jumps loopStart while rows remain, falls through when done
	b.EmitABC(BcNext, 0, 0, int32(loopStart))

	b.FixupLabel(lblEnd)
	b.Emit(BcHalt)

	prog := b.Build()
	vm := NewBytecodeVM(prog, ctx)
	if err := vm.Run(); err != nil {
		t.Fatal(err)
	}
	rows := vm.ResultRows()
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	for i, want := range []int64{1, 2, 3} {
		if rows[i][0] != want {
			t.Errorf("row[%d] = %v, want %d", i, rows[i][0], want)
		}
	}
}

func TestBytecodeVM_ResultRow(t *testing.T) {
	b := NewBytecodeBuilder()
	b.SetColNames([]string{"a", "b"})
	r0 := b.AllocReg()
	r1 := b.AllocReg()
	c0 := b.AddConst(VmInt(7))
	c1 := b.AddConst(VmText("hello"))
	b.EmitABC(BcLoadConst, 0, c0, r0)
	b.EmitABC(BcLoadConst, 0, c1, r1)
	b.EmitAB(BcResultRow, r0, 2)
	b.Emit(BcHalt)

	prog := b.Build()
	vm := NewBytecodeVM(prog, nil)
	if err := vm.Run(); err != nil {
		t.Fatal(err)
	}
	rows := vm.ResultRows()
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0][0] != int64(7) {
		t.Errorf("col0 = %v, want 7", rows[0][0])
	}
	if rows[0][1] != "hello" {
		t.Errorf("col1 = %v, want hello", rows[0][1])
	}
}
