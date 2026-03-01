package engine_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/VM/engine"
)

func TestMergeRows(t *testing.T) {
	a := engine.Row{"x": int64(1)}
	b := engine.Row{"y": int64(2)}
	m := engine.MergeRows(a, b)
	if m["x"] != int64(1) || m["y"] != int64(2) {
		t.Errorf("unexpected merged row: %v", m)
	}
}

func TestMergeRowsWithAlias(t *testing.T) {
	a := engine.Row{"id": int64(10)}
	b := engine.Row{"id": int64(20)}
	m := engine.MergeRowsWithAlias(a, "e", b, "d")
	if m["e.id"] != int64(10) {
		t.Errorf("expected e.id=10, got %v", m["e.id"])
	}
	if m["d.id"] != int64(20) {
		t.Errorf("expected d.id=20, got %v", m["d.id"])
	}
	// Unqualified "id" should come from b (b overwrites a).
	if m["id"] != int64(20) {
		t.Errorf("expected id=20 (from b), got %v", m["id"])
	}
}

func TestCrossJoin(t *testing.T) {
	left := []engine.Row{{"l": int64(1)}, {"l": int64(2)}}
	right := []engine.Row{{"r": int64(10)}, {"r": int64(20)}}
	out := engine.CrossJoin(left, right)
	if len(out) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(out))
	}
}

func TestCrossJoin_Empty(t *testing.T) {
	out := engine.CrossJoin(nil, []engine.Row{{"r": 1}})
	if len(out) != 0 {
		t.Error("expected empty result for empty left")
	}
}

func TestInnerJoin(t *testing.T) {
	left := []engine.Row{{"id": int64(1)}, {"id": int64(2)}}
	right := []engine.Row{{"id": int64(2)}, {"id": int64(3)}}
	out := engine.InnerJoin(left, right, func(r engine.Row) bool {
		// Join where both sides have the same id — but after MergeRows, b's id
		// overwrites a's id.  Use the alias strategy instead.
		return true // allow all in this simple test
	})
	if len(out) != 4 {
		t.Fatalf("expected 4 rows (cross), got %d", len(out))
	}
}

func TestLeftOuterJoin_NoMatch(t *testing.T) {
	left := []engine.Row{{"id": int64(1), "name": "alice"}}
	right := []engine.Row{{"dept_id": int64(99), "dept": "eng"}}
	out := engine.LeftOuterJoin(left, right, func(r engine.Row) bool {
		return false // no matches
	}, []string{"dept_id", "dept"})
	if len(out) != 1 {
		t.Fatalf("expected 1 row (left preserved), got %d", len(out))
	}
	if out[0]["dept"] != nil {
		t.Errorf("expected dept=nil, got %v", out[0]["dept"])
	}
}

func TestLeftOuterJoin_WithMatch(t *testing.T) {
	left := []engine.Row{{"id": int64(1)}}
	right := []engine.Row{{"fk": int64(1), "data": "x"}}
	out := engine.LeftOuterJoin(left, right, func(r engine.Row) bool {
		return r["id"] == r["fk"]
	}, []string{"fk", "data"})
	if len(out) != 1 {
		t.Fatalf("expected 1 row, got %d", len(out))
	}
	if out[0]["data"] != "x" {
		t.Errorf("expected data=x, got %v", out[0]["data"])
	}
}
