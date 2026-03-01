package window_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	"github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/window"
)

func makeTestRowSet(values []int64) *window.RowSet {
	rs := &window.RowSet{Columns: []string{"v"}}
	for _, v := range values {
		rs.Data = append(rs.Data, []interface{}{v})
	}
	return rs
}

func TestComputeRankValues_Basic(t *testing.T) {
	rs := makeTestRowSet([]int64{3, 1, 2, 1})
	wf := &QP.WindowFuncExpr{
		Name:    "RANK",
		OrderBy: []QP.WindowOrderBy{{Expr: &QP.ColumnRef{Name: "v"}}},
	}
	ranks := window.ComputeRankValues(rs, wf, false)
	// Sorted: 1,1,2,3 → ranks 1,1,3,4
	// Original order: v=3→rank 4, v=1→rank 1, v=2→rank 3, v=1→rank 1
	if ranks[1] != int64(1) {
		t.Errorf("v=1 should have rank 1, got %v", ranks[1])
	}
	if ranks[0] != int64(4) {
		t.Errorf("v=3 should have rank 4, got %v", ranks[0])
	}
}

func TestComputeRankValues_Dense(t *testing.T) {
	rs := makeTestRowSet([]int64{3, 1, 2, 1})
	wf := &QP.WindowFuncExpr{
		Name:    "DENSE_RANK",
		OrderBy: []QP.WindowOrderBy{{Expr: &QP.ColumnRef{Name: "v"}}},
	}
	ranks := window.ComputeRankValues(rs, wf, true)
	// Dense ranks: 1→1, 2→2, 3→3
	if ranks[0] != int64(3) {
		t.Errorf("v=3 should have dense_rank 3, got %v", ranks[0])
	}
	if ranks[1] != int64(1) {
		t.Errorf("v=1 should have dense_rank 1, got %v", ranks[1])
	}
}

func TestComputeRankFloat_PercentRank(t *testing.T) {
	rs := makeTestRowSet([]int64{1, 2, 3})
	wf := &QP.WindowFuncExpr{
		Name:    "PERCENT_RANK",
		OrderBy: []QP.WindowOrderBy{{Expr: &QP.ColumnRef{Name: "v"}}},
	}
	ranks := window.ComputeRankFloat(rs, wf)
	// Sorted order 1,2,3: percent_ranks 0.0, 0.5, 1.0
	if ranks[0] != float64(0) {
		t.Errorf("v=1 percent_rank should be 0.0, got %v", ranks[0])
	}
	if ranks[2] != float64(1) {
		t.Errorf("v=3 percent_rank should be 1.0, got %v", ranks[2])
	}
}

func TestComputeCumeDist(t *testing.T) {
	rs := makeTestRowSet([]int64{1, 2, 3})
	wf := &QP.WindowFuncExpr{
		Name:    "CUME_DIST",
		OrderBy: []QP.WindowOrderBy{{Expr: &QP.ColumnRef{Name: "v"}}},
	}
	dists := window.ComputeCumeDist(rs, wf)
	// 1→1/3, 2→2/3, 3→3/3=1
	if dists[0] != float64(1)/3 {
		t.Errorf("v=1 cume_dist should be 1/3, got %v", dists[0])
	}
	if dists[2] != float64(1) {
		t.Errorf("v=3 cume_dist should be 1.0, got %v", dists[2])
	}
}

func TestComputeOrderedWindowValues_RowNumber(t *testing.T) {
	rs := makeTestRowSet([]int64{10, 30, 20})
	wf := &QP.WindowFuncExpr{
		Name:    "ROW_NUMBER",
		OrderBy: []QP.WindowOrderBy{{Expr: &QP.ColumnRef{Name: "v"}}},
	}
	nums := window.ComputeOrderedWindowValues(rs, wf, func(sortedIndices []int, pos int) interface{} {
		return int64(pos + 1)
	})
	// Sorted order: 10, 20, 30 → row nums 1, 2, 3
	// Original: v=10→1, v=30→3, v=20→2
	if nums[0] != int64(1) {
		t.Errorf("v=10 row_number should be 1, got %v", nums[0])
	}
	if nums[1] != int64(3) {
		t.Errorf("v=30 row_number should be 3, got %v", nums[1])
	}
}

func TestComputeLag(t *testing.T) {
	rs := makeTestRowSet([]int64{1, 2, 3})
	wf := &QP.WindowFuncExpr{
		Name:    "LAG",
		Args:    []QP.Expr{&QP.ColumnRef{Name: "v"}},
		OrderBy: []QP.WindowOrderBy{{Expr: &QP.ColumnRef{Name: "v"}}},
	}
	vals := window.ComputeLag(rs, wf)
	// Sorted: 1,2,3 → lag: nil,1,2
	if vals[0] != nil {
		t.Errorf("v=1 lag should be nil, got %v", vals[0])
	}
	if vals[1] != int64(1) {
		t.Errorf("v=2 lag should be 1, got %v", vals[1])
	}
}
