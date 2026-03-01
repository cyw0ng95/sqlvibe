package engine_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/VM/engine"
)

func cmpIntForSort(a, b interface{}) int {
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

func makeRows(ids ...int64) []engine.Row {
	rows := make([]engine.Row, len(ids))
	for i, id := range ids {
		rows[i] = engine.Row{"id": id}
	}
	return rows
}

func TestSortRowsByKeys_Ascending(t *testing.T) {
	rows := makeRows(3, 1, 2)
	keys := []engine.SortKey{{ColName: "id", Order: engine.Ascending}}
	sorted := engine.SortRowsByKeys(rows, keys, cmpIntForSort)
	for i, expected := range []int64{1, 2, 3} {
		if sorted[i]["id"].(int64) != expected {
			t.Errorf("position %d: expected %d, got %v", i, expected, sorted[i]["id"])
		}
	}
}

func TestSortRowsByKeys_Descending(t *testing.T) {
	rows := makeRows(3, 1, 2)
	keys := []engine.SortKey{{ColName: "id", Order: engine.Descending}}
	sorted := engine.SortRowsByKeys(rows, keys, cmpIntForSort)
	for i, expected := range []int64{3, 2, 1} {
		if sorted[i]["id"].(int64) != expected {
			t.Errorf("position %d: expected %d, got %v", i, expected, sorted[i]["id"])
		}
	}
}

func TestSortRowsByKeys_NullsFirst(t *testing.T) {
	rows := []engine.Row{{"id": int64(2)}, {"id": nil}, {"id": int64(1)}}
	keys := []engine.SortKey{{ColName: "id", Order: engine.Ascending, NullOrder: engine.NullsFirst}}
	sorted := engine.SortRowsByKeys(rows, keys, cmpIntForSort)
	if sorted[0]["id"] != nil {
		t.Errorf("expected nil first, got %v", sorted[0]["id"])
	}
}

func TestSortRowsByKeys_NullsLast(t *testing.T) {
	rows := []engine.Row{{"id": int64(2)}, {"id": nil}, {"id": int64(1)}}
	keys := []engine.SortKey{{ColName: "id", Order: engine.Ascending, NullOrder: engine.NullsLast}}
	sorted := engine.SortRowsByKeys(rows, keys, cmpIntForSort)
	if sorted[len(sorted)-1]["id"] != nil {
		t.Errorf("expected nil last, got %v", sorted[len(sorted)-1]["id"])
	}
}

func TestTopKRows(t *testing.T) {
	rows := makeRows(5, 3, 1, 4, 2)
	keys := []engine.SortKey{{ColName: "id", Order: engine.Ascending}}
	top3 := engine.TopKRows(rows, 3, keys, cmpIntForSort)
	if len(top3) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(top3))
	}
	if top3[0]["id"].(int64) != 1 {
		t.Errorf("expected 1 first, got %v", top3[0]["id"])
	}
}

func TestSortRowsByKeys_Empty(t *testing.T) {
	sorted := engine.SortRowsByKeys(nil, nil, cmpIntForSort)
	if len(sorted) != 0 {
		t.Error("expected empty result for nil input")
	}
}

func TestReverseRows(t *testing.T) {
	rows := makeRows(1, 2, 3)
	rev := engine.ReverseRows(rows)
	if rev[0]["id"].(int64) != 3 {
		t.Errorf("expected 3 first after reverse, got %v", rev[0]["id"])
	}
}
