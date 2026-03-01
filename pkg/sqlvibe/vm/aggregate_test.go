package vm_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	svvm "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/vm"
)

func TestIsSimpleAggregate_CountStar(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.FuncCall{Name: "count", Args: []QP.Expr{&QP.ColumnRef{Name: "*"}}}},
		From:    &QP.TableRef{Name: "t"},
	}
	fn, col, ok := svvm.IsSimpleAggregate(stmt)
	if !ok || fn != "COUNT" || col != "*" {
		t.Errorf("expected COUNT/*; got fn=%q col=%q ok=%v", fn, col, ok)
	}
}

func TestIsSimpleAggregate_Sum(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.FuncCall{Name: "sum", Args: []QP.Expr{&QP.ColumnRef{Name: "price"}}}},
		From:    &QP.TableRef{Name: "t"},
	}
	fn, col, ok := svvm.IsSimpleAggregate(stmt)
	if !ok || fn != "SUM" || col != "price" {
		t.Errorf("expected SUM/price; got fn=%q col=%q ok=%v", fn, col, ok)
	}
}

func TestIsSimpleAggregate_WithWhere_False(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.FuncCall{Name: "count", Args: []QP.Expr{&QP.ColumnRef{Name: "*"}}}},
		From:    &QP.TableRef{Name: "t"},
		Where:   &QP.ColumnRef{Name: "id"},
	}
	_, _, ok := svvm.IsSimpleAggregate(stmt)
	if ok {
		t.Error("expected false when WHERE is present")
	}
}

func TestIsSimpleAggregate_GroupBy_False(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.FuncCall{Name: "sum", Args: []QP.Expr{&QP.ColumnRef{Name: "x"}}}},
		From:    &QP.TableRef{Name: "t"},
		GroupBy: []QP.Expr{&QP.ColumnRef{Name: "cat"}},
	}
	_, _, ok := svvm.IsSimpleAggregate(stmt)
	if ok {
		t.Error("expected false when GROUP BY is present")
	}
}

func TestIsVectorizedFilterEligible_Simple(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.ColumnRef{Name: "*"}},
		From:    &QP.TableRef{Name: "t"},
		Where: &QP.BinaryExpr{
			Op:    QP.TokenEq,
			Left:  &QP.ColumnRef{Name: "id"},
			Right: &QP.Literal{Value: int64(1)},
		},
	}
	if !svvm.IsVectorizedFilterEligible(stmt) {
		t.Error("expected true for simple col=val WHERE")
	}
}

func TestIsVectorizedFilterEligible_NoWhere(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.ColumnRef{Name: "*"}},
		From:    &QP.TableRef{Name: "t"},
	}
	if svvm.IsVectorizedFilterEligible(stmt) {
		t.Error("expected false when WHERE is absent")
	}
}

func TestIsVectorizedFilterEligible_FuncInSelect(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.FuncCall{Name: "upper", Args: []QP.Expr{&QP.ColumnRef{Name: "name"}}}},
		From:    &QP.TableRef{Name: "t"},
		Where: &QP.BinaryExpr{
			Op:    QP.TokenEq,
			Left:  &QP.ColumnRef{Name: "id"},
			Right: &QP.Literal{Value: int64(1)},
		},
	}
	if svvm.IsVectorizedFilterEligible(stmt) {
		t.Error("expected false when SELECT has func call")
	}
}

func TestExtractFilterInfo_EqInt(t *testing.T) {
	stmt := &QP.SelectStmt{
		Columns: []QP.Expr{&QP.ColumnRef{Name: "*"}},
		From:    &QP.TableRef{Name: "t"},
		Where: &QP.BinaryExpr{
			Op:    QP.TokenEq,
			Left:  &QP.ColumnRef{Name: "id"},
			Right: &QP.Literal{Value: int64(5)},
		},
	}
	info := svvm.ExtractFilterInfo(stmt)
	if info == nil {
		t.Fatal("expected non-nil FilterInfo")
	}
	if info.ColName != "id" || info.Op != "=" {
		t.Errorf("unexpected filter: col=%q op=%q", info.ColName, info.Op)
	}
}
