package engine_test

import (
	"strings"
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/VM/engine"
)

func TestFilterRows_Nil(t *testing.T) {
	rows := []engine.Row{{"id": int64(1)}, {"id": int64(2)}}
	got := engine.FilterRows(rows, nil)
	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
}

func TestFilterRows_Predicate(t *testing.T) {
	rows := []engine.Row{
		{"id": int64(1), "v": "a"},
		{"id": int64(2), "v": "b"},
		{"id": int64(3), "v": "a"},
	}
	got := engine.FilterRows(rows, func(r engine.Row) bool {
		return r["v"] == "a"
	})
	if len(got) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(got))
	}
}

func TestProjectRow(t *testing.T) {
	row := engine.Row{"x": int64(10), "y": int64(20)}
	proj := map[string]func(engine.Row) interface{}{
		"sum": func(r engine.Row) interface{} {
			return r["x"].(int64) + r["y"].(int64)
		},
	}
	out := engine.ProjectRow(row, proj)
	if out["sum"] != int64(30) {
		t.Errorf("expected sum=30, got %v", out["sum"])
	}
}

func TestProjectRows(t *testing.T) {
	rows := []engine.Row{
		{"name": "alice"},
		{"name": "bob"},
	}
	proj := map[string]func(engine.Row) interface{}{
		"upper": func(r engine.Row) interface{} {
			return strings.ToUpper(r["name"].(string))
		},
	}
	out := engine.ProjectRows(rows, proj)
	if len(out) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(out))
	}
	if out[0]["upper"] != "ALICE" {
		t.Errorf("expected ALICE, got %v", out[0]["upper"])
	}
}

func TestApplyDistinct(t *testing.T) {
	rows := []engine.Row{
		{"dept": "eng"},
		{"dept": "sales"},
		{"dept": "eng"},
	}
	got := engine.ApplyDistinct(rows, func(r engine.Row) string {
		return r["dept"].(string)
	})
	if len(got) != 2 {
		t.Fatalf("expected 2 distinct rows, got %d", len(got))
	}
}

func TestApplyLimitOffset(t *testing.T) {
	rows := make([]engine.Row, 10)
	for i := range rows {
		rows[i] = engine.Row{"i": int64(i)}
	}
	got := engine.ApplyLimitOffset(rows, 3, 2)
	if len(got) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(got))
	}
	if got[0]["i"] != int64(2) {
		t.Errorf("expected first row i=2, got %v", got[0]["i"])
	}
}

func TestApplyLimitOffset_OffsetBeyondEnd(t *testing.T) {
	rows := []engine.Row{{"id": int64(1)}}
	got := engine.ApplyLimitOffset(rows, 10, 5)
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestColNames(t *testing.T) {
	rows := []engine.Row{
		{"a": 1, "b": 2},
		{"b": 3, "c": 4},
	}
	names := engine.ColNames(rows)
	if len(names) < 3 {
		t.Errorf("expected at least 3 col names, got %v", names)
	}
}
