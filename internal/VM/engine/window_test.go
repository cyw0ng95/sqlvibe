package engine_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/VM/engine"
)

func intLess(a, b engine.Row) bool {
	ai, _ := a["n"].(int64)
	bi, _ := b["n"].(int64)
	return ai < bi
}

func makeNRows(vals ...int64) []engine.Row {
	rows := make([]engine.Row, len(vals))
	for i, v := range vals {
		rows[i] = engine.Row{"n": v}
	}
	return rows
}

func TestRowNumbers(t *testing.T) {
	rows := makeNRows(10, 20, 30)
	nums := engine.RowNumbers(rows)
	for i, expected := range []int64{1, 2, 3} {
		if nums[i] != expected {
			t.Errorf("position %d: expected %d, got %d", i, expected, nums[i])
		}
	}
}

func TestPartitionRows(t *testing.T) {
	rows := []engine.Row{
		{"dept": "eng", "n": int64(1)},
		{"dept": "hr", "n": int64(2)},
		{"dept": "eng", "n": int64(3)},
	}
	parts := engine.PartitionRows(rows, func(r engine.Row) string {
		return r["dept"].(string)
	})
	if len(parts) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(parts))
	}
}

func TestRanks(t *testing.T) {
	rows := makeNRows(1, 1, 3)
	ranks := engine.Ranks(rows, intLess)
	// rows[0] and rows[1] tie at rank 1; rows[2] gets rank 3.
	if ranks[0] != 1 || ranks[1] != 1 {
		t.Errorf("tied rows should both be rank 1, got %v %v", ranks[0], ranks[1])
	}
	if ranks[2] != 3 {
		t.Errorf("third row should be rank 3, got %v", ranks[2])
	}
}

func TestDenseRanks(t *testing.T) {
	rows := makeNRows(1, 1, 3)
	dr := engine.DenseRanks(rows, intLess)
	if dr[0] != 1 || dr[1] != 1 {
		t.Errorf("tied rows should both be dense rank 1, got %v %v", dr[0], dr[1])
	}
	if dr[2] != 2 {
		t.Errorf("third row should be dense rank 2, got %v", dr[2])
	}
}

func TestLagValues(t *testing.T) {
	rows := makeNRows(10, 20, 30)
	colFn := func(r engine.Row) interface{} { return r["n"] }
	lags := engine.LagValues(rows, colFn, 1, nil)
	if lags[0] != nil {
		t.Errorf("expected nil for first lag, got %v", lags[0])
	}
	if lags[1].(int64) != 10 {
		t.Errorf("expected lag=10 at pos 1, got %v", lags[1])
	}
}

func TestLeadValues(t *testing.T) {
	rows := makeNRows(10, 20, 30)
	colFn := func(r engine.Row) interface{} { return r["n"] }
	leads := engine.LeadValues(rows, colFn, 1, nil)
	if leads[0].(int64) != 20 {
		t.Errorf("expected lead=20 at pos 0, got %v", leads[0])
	}
	if leads[2] != nil {
		t.Errorf("expected nil for last lead, got %v", leads[2])
	}
}

func TestNthValues(t *testing.T) {
	rows := makeNRows(10, 20, 30)
	colFn := func(r engine.Row) interface{} { return r["n"] }
	nth := engine.NthValues(rows, colFn, 2)
	if nth[0] != nil {
		t.Errorf("expected nil before n=2 is available, got %v", nth[0])
	}
	if nth[1].(int64) != 20 {
		t.Errorf("expected nth=20 at pos 1, got %v", nth[1])
	}
}
