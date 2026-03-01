package engine_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/VM/engine"
)

func cmpInt(a, b interface{}) int {
	ai, _ := a.(int64)
	bi, _ := b.(int64)
	if ai < bi {
		return -1
	}
	if ai > bi {
		return 1
	}
	return 0
}

func TestCountRows_All(t *testing.T) {
	rows := []engine.Row{{"v": int64(1)}, {"v": int64(2)}, {"v": nil}}
	if engine.CountRows(rows, "") != 3 {
		t.Error("expected 3 total rows")
	}
}

func TestCountRows_NonNull(t *testing.T) {
	rows := []engine.Row{{"v": int64(1)}, {"v": nil}, {"v": int64(3)}}
	if engine.CountRows(rows, "v") != 2 {
		t.Error("expected 2 non-nil rows")
	}
}

func TestSumRows(t *testing.T) {
	rows := []engine.Row{
		{"n": int64(10)},
		{"n": int64(20)},
		{"n": nil},
	}
	got := engine.SumRows(rows, "n")
	if got.(float64) != 30.0 {
		t.Errorf("expected 30.0, got %v", got)
	}
}

func TestSumRows_Empty(t *testing.T) {
	got := engine.SumRows(nil, "n")
	if got != nil {
		t.Errorf("expected nil for empty input, got %v", got)
	}
}

func TestAvgRows(t *testing.T) {
	rows := []engine.Row{{"n": int64(10)}, {"n": int64(20)}, {"n": int64(30)}}
	got := engine.AvgRows(rows, "n")
	if got.(float64) != 20.0 {
		t.Errorf("expected 20.0, got %v", got)
	}
}

func TestMinRows(t *testing.T) {
	rows := []engine.Row{{"n": int64(5)}, {"n": int64(2)}, {"n": int64(8)}}
	got := engine.MinRows(rows, "n", cmpInt)
	if got.(int64) != 2 {
		t.Errorf("expected 2, got %v", got)
	}
}

func TestMaxRows(t *testing.T) {
	rows := []engine.Row{{"n": int64(5)}, {"n": int64(2)}, {"n": int64(8)}}
	got := engine.MaxRows(rows, "n", cmpInt)
	if got.(int64) != 8 {
		t.Errorf("expected 8, got %v", got)
	}
}

func TestGroupRows(t *testing.T) {
	rows := []engine.Row{
		{"dept": "eng", "sal": int64(100)},
		{"dept": "eng", "sal": int64(200)},
		{"dept": "hr", "sal": int64(150)},
	}
	groups := engine.GroupRows(rows, func(r engine.Row) string {
		return r["dept"].(string)
	})
	if len(groups["eng"]) != 2 {
		t.Errorf("expected 2 eng rows, got %d", len(groups["eng"]))
	}
	if len(groups["hr"]) != 1 {
		t.Errorf("expected 1 hr row, got %d", len(groups["hr"]))
	}
}

func TestGroupByAndAggregate(t *testing.T) {
	rows := []engine.Row{
		{"dept": "eng", "sal": int64(100)},
		{"dept": "eng", "sal": int64(200)},
		{"dept": "hr", "sal": int64(150)},
	}
	result := engine.GroupByAndAggregate(
		rows,
		func(r engine.Row) string { return r["dept"].(string) },
		func(key string, group []engine.Row) engine.Row {
			total := engine.SumRows(group, "sal")
			return engine.Row{"dept": key, "total": total}
		},
	)
	if len(result) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(result))
	}
}
