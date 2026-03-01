package window_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/window"
)

func TestBuildPartitionGroups_NoPartition(t *testing.T) {
	rs := &window.RowSet{
		Columns: []string{"v"},
		Data:    [][]interface{}{{int64(1)}, {int64(2)}, {int64(3)}},
	}
	groups := window.BuildPartitionGroups(rs, nil)
	if len(groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(groups))
	}
	if len(groups[0]) != 3 {
		t.Errorf("group should have 3 rows, got %d", len(groups[0]))
	}
}

func TestBuildPartitionGroups_WithPartition(t *testing.T) {
	rs := &window.RowSet{
		Columns: []string{"dept", "sal"},
		Data: [][]interface{}{
			{"eng", int64(100)},
			{"hr", int64(90)},
			{"eng", int64(110)},
		},
	}
	partExprs := []QP.Expr{&QP.ColumnRef{Name: "dept"}}
	groups := window.BuildPartitionGroups(rs, partExprs)
	if len(groups) != 2 {
		t.Fatalf("expected 2 partition groups, got %d", len(groups))
	}
}

func TestSortRowIndices_Ascending(t *testing.T) {
	rs := &window.RowSet{
		Columns: []string{"v"},
		Data:    [][]interface{}{{int64(3)}, {int64(1)}, {int64(2)}},
	}
	indices := []int{0, 1, 2}
	ob := []QP.WindowOrderBy{{Expr: &QP.ColumnRef{Name: "v"}, Desc: false}}
	sorted := window.SortRowIndices(rs, indices, ob)
	// Should be [1, 2, 0] (values 1, 2, 3)
	if rs.Data[sorted[0]][0] != int64(1) {
		t.Errorf("first sorted row should have v=1, got %v", rs.Data[sorted[0]][0])
	}
}

func TestSortRowIndices_Descending(t *testing.T) {
	rs := &window.RowSet{
		Columns: []string{"v"},
		Data:    [][]interface{}{{int64(3)}, {int64(1)}, {int64(2)}},
	}
	indices := []int{0, 1, 2}
	ob := []QP.WindowOrderBy{{Expr: &QP.ColumnRef{Name: "v"}, Desc: true}}
	sorted := window.SortRowIndices(rs, indices, ob)
	if rs.Data[sorted[0]][0] != int64(3) {
		t.Errorf("first sorted desc row should have v=3, got %v", rs.Data[sorted[0]][0])
	}
}

func TestSameOrderKey_Equal(t *testing.T) {
	rs := &window.RowSet{
		Columns: []string{"v"},
		Data:    [][]interface{}{{int64(5)}, {int64(5)}},
	}
	ob := []QP.WindowOrderBy{{Expr: &QP.ColumnRef{Name: "v"}}}
	if !window.SameOrderKey(rs, 0, 1, ob) {
		t.Error("rows with same value should have same order key")
	}
}

func TestSameOrderKey_Different(t *testing.T) {
	rs := &window.RowSet{
		Columns: []string{"v"},
		Data:    [][]interface{}{{int64(5)}, {int64(6)}},
	}
	ob := []QP.WindowOrderBy{{Expr: &QP.ColumnRef{Name: "v"}}}
	if window.SameOrderKey(rs, 0, 1, ob) {
		t.Error("rows with different values should not have same order key")
	}
}

func TestCompareVals_NullFirst(t *testing.T) {
	if window.CompareVals(nil, int64(1)) >= 0 {
		t.Error("nil should sort before non-nil")
	}
}

func TestMakeRowMap(t *testing.T) {
	columns := []string{"a", "b"}
	data := []interface{}{int64(1), "hello"}
	row := window.MakeRowMap(columns, data)
	if row["a"] != int64(1) || row["b"] != "hello" {
		t.Errorf("MakeRowMap result unexpected: %v", row)
	}
}
