package QE

import (
	"testing"
)

func TestVMCreate(t *testing.T) {
	instructions := []Instruction{
		{Op: OpOpenRead, P1: 0},
		{Op: OpNext, P1: 0},
	}
	vm := NewVM(instructions, 10)
	if vm == nil {
		t.Error("VM should not be nil")
	}
	if vm.pc != 0 {
		t.Error("PC should start at 0")
	}
}

func TestVMRun(t *testing.T) {
	instructions := []Instruction{
		{Op: OpInteger, P1: 0, P3: "1"},
		{Op: OpInteger, P1: 1, P3: "2"},
		{Op: OpAdd, P1: 0, P2: 1, P3: "2"},
	}
	vm := NewVM(instructions, 10)
	vm.SetRegister(0, 10)
	vm.SetRegister(1, 20)

	evaluator := NewExprEvaluator(vm)
	result, _ := evaluator.BinaryOp(OpAdd, 10, 20)
	if result != int(30) && result != float64(30) {
		t.Errorf("expected 30, got %v", result)
	}
}

func TestExprEvaluator(t *testing.T) {
	vm := NewVM(nil, 10)
	evaluator := NewExprEvaluator(vm)

	result := evaluator.toFloat64(10)
	if result != 10.0 {
		t.Errorf("expected 10.0, got %v", result)
	}

	result = evaluator.toFloat64("10.5")
	if result != 10.5 {
		t.Errorf("expected 10.5, got %v", result)
	}
}

func TestAggregatorCount(t *testing.T) {
	agg := NewCountAgg()
	agg.Step(nil)
	agg.Step(1)
	agg.Step(2)
	result := agg.Result()
	if result != int64(3) {
		t.Errorf("expected 3, got %v", result)
	}
}

func TestAggregatorSum(t *testing.T) {
	agg := NewSumAgg()
	agg.Step(10)
	agg.Step(20)
	agg.Step(30)
	result := agg.Result()
	if result != float64(60) {
		t.Errorf("expected 60.0, got %v", result)
	}
}

func TestAggregatorAvg(t *testing.T) {
	agg := NewAvgAgg()
	agg.Step(10)
	agg.Step(20)
	agg.Step(30)
	result := agg.Result()
	if result != float64(20) {
		t.Errorf("expected 20.0, got %v", result)
	}
}

func TestResultSet(t *testing.T) {
	rs := NewResultSet([]string{"id", "name"})
	rs.AddRow([]interface{}{1, "Alice"})
	rs.AddRow([]interface{}{2, "Bob"})

	cols := rs.Columns()
	if len(cols) != 2 {
		t.Errorf("expected 2 columns, got %d", len(cols))
	}

	if !rs.Next() {
		t.Error("expected first row")
	}
	row := rs.Get()
	if row[0].(int) != 1 {
		t.Errorf("expected id=1, got %v", row[0])
	}
}

func TestQueryEngine(t *testing.T) {
	qe := NewQueryEngine()
	qe.RegisterTable("users", map[string]ColumnType{
		"id":   {Name: "id", Type: "INTEGER"},
		"name": {Name: "name", Type: "TEXT"},
	})

	cursorID, err := qe.OpenCursor("users")
	if err != nil {
		t.Fatalf("failed to open cursor: %v", err)
	}
	if cursorID < 0 {
		t.Error("expected valid cursor ID")
	}

	row, err := qe.NextRow(cursorID)
	if err != nil {
		t.Fatalf("failed to get next row: %v", err)
	}
	t.Logf("row: %v", row)
}
