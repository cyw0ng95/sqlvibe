package vm_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	svvm "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/vm"
)

func TestCollectColumnRefs_Nil(t *testing.T) {
	refs := svvm.CollectColumnRefs(nil)
	if len(refs) != 0 {
		t.Errorf("expected 0 refs for nil, got %v", refs)
	}
}

func TestCollectColumnRefs_Simple(t *testing.T) {
	refs := svvm.CollectColumnRefs(&QP.ColumnRef{Name: "id"})
	if len(refs) != 1 || refs[0] != "id" {
		t.Errorf("expected [id], got %v", refs)
	}
}

func TestCollectColumnRefs_Star_Excluded(t *testing.T) {
	refs := svvm.CollectColumnRefs(&QP.ColumnRef{Name: "*"})
	if len(refs) != 0 {
		t.Errorf("expected * to be excluded, got %v", refs)
	}
}

func TestCollectColumnRefs_BinaryExpr(t *testing.T) {
	expr := &QP.BinaryExpr{
		Op:    QP.TokenPlus,
		Left:  &QP.ColumnRef{Name: "a"},
		Right: &QP.ColumnRef{Name: "b"},
	}
	refs := svvm.CollectColumnRefs(expr)
	if len(refs) != 2 {
		t.Errorf("expected 2 refs, got %v", refs)
	}
}

func TestCollectColumnRefs_FuncCall(t *testing.T) {
	expr := &QP.FuncCall{
		Name: "max",
		Args: []QP.Expr{&QP.ColumnRef{Name: "price"}},
	}
	refs := svvm.CollectColumnRefs(expr)
	if len(refs) != 1 || refs[0] != "price" {
		t.Errorf("expected [price], got %v", refs)
	}
}

func TestCollectColumnRefs_Literal_NoRefs(t *testing.T) {
	refs := svvm.CollectColumnRefs(&QP.Literal{Value: int64(42)})
	if len(refs) != 0 {
		t.Errorf("expected 0 refs for literal, got %v", refs)
	}
}

func TestCollectColumnRefs_UnaryExpr(t *testing.T) {
	expr := &QP.UnaryExpr{Op: QP.TokenMinus, Expr: &QP.ColumnRef{Name: "x"}}
	refs := svvm.CollectColumnRefs(expr)
	if len(refs) != 1 || refs[0] != "x" {
		t.Errorf("expected [x], got %v", refs)
	}
}

func TestCollectColumnRefs_AliasExpr(t *testing.T) {
	expr := &QP.AliasExpr{Alias: "total", Expr: &QP.ColumnRef{Name: "price"}}
	refs := svvm.CollectColumnRefs(expr)
	if len(refs) != 1 || refs[0] != "price" {
		t.Errorf("expected [price], got %v", refs)
	}
}
