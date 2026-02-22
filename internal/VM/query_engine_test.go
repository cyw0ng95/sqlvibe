package VM

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/DS"
	"github.com/cyw0ng95/sqlvibe/internal/PB"
)

func TestVMCreate(t *testing.T) {
	program := NewProgram()
	vm := NewVM(program)
	if vm == nil {
		t.Error("VM should not be nil")
	}
}

func TestVMRun(t *testing.T) {
	program := NewProgram()
	vm := NewVM(program)
	if vm == nil {
		t.Error("VM should not be nil")
	}
	if vm.PC() != 0 {
		t.Error("PC should start at 0")
	}
}

func TestExprEvaluator(t *testing.T) {
	vm := NewVM(NewProgram())
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
	file, err := PB.OpenFile(":memory:", PB.O_CREATE|PB.O_RDWR)
	if err != nil {
		t.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	pm, err := DS.NewPageManager(file, 4096)
	if err != nil {
		t.Fatalf("failed to create page manager: %v", err)
	}

	qe := NewQueryEngine(pm, nil)
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
