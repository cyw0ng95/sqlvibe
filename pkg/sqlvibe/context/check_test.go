package context_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	svctx "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/context"
)

func TestEvalCheckExpr_Literal(t *testing.T) {
	expr := &QP.Literal{Value: int64(42)}
	result := svctx.EvalCheckExpr(expr, nil)
	if result != int64(42) {
		t.Errorf("EvalCheckExpr(42) = %v, want 42", result)
	}
}

func TestEvalCheckExpr_ColumnRef(t *testing.T) {
	expr := &QP.ColumnRef{Name: "age"}
	row := map[string]interface{}{"age": int64(25)}
	result := svctx.EvalCheckExpr(expr, row)
	if result != int64(25) {
		t.Errorf("EvalCheckExpr(column) = %v, want 25", result)
	}
}

func TestEvalCheckExpr_BinaryGt(t *testing.T) {
	expr := &QP.BinaryExpr{
		Op:    QP.TokenGt,
		Left:  &QP.ColumnRef{Name: "age"},
		Right: &QP.Literal{Value: int64(0)},
	}
	row := map[string]interface{}{"age": int64(25)}
	result := svctx.EvalCheckExpr(expr, row)
	if b, ok := result.(bool); !ok || !b {
		t.Errorf("EvalCheckExpr(age > 0) = %v, want true", result)
	}
}

func TestEvalCheckExpr_BinaryLe(t *testing.T) {
	expr := &QP.BinaryExpr{
		Op:    QP.TokenLe,
		Left:  &QP.ColumnRef{Name: "score"},
		Right: &QP.Literal{Value: int64(100)},
	}
	row := map[string]interface{}{"score": int64(85)}
	result := svctx.EvalCheckExpr(expr, row)
	if b, ok := result.(bool); !ok || !b {
		t.Errorf("EvalCheckExpr(score <= 100) = %v, want true", result)
	}
}

func TestEvalCheckExpr_NullPropagation(t *testing.T) {
	expr := &QP.BinaryExpr{
		Op:    QP.TokenEq,
		Left:  &QP.ColumnRef{Name: "missing"},
		Right: &QP.Literal{Value: int64(5)},
	}
	row := map[string]interface{}{}
	result := svctx.EvalCheckExpr(expr, row)
	if result != nil {
		t.Errorf("EvalCheckExpr(NULL = 5) = %v, want nil", result)
	}
}

func TestCheckPasses_True(t *testing.T) {
	expr := &QP.BinaryExpr{
		Op:    QP.TokenGt,
		Left:  &QP.ColumnRef{Name: "qty"},
		Right: &QP.Literal{Value: int64(0)},
	}
	row := map[string]interface{}{"qty": int64(10)}
	if !svctx.CheckPasses(expr, row) {
		t.Error("CheckPasses(qty > 0) should be true")
	}
}

func TestCheckPasses_False(t *testing.T) {
	expr := &QP.BinaryExpr{
		Op:    QP.TokenGt,
		Left:  &QP.ColumnRef{Name: "qty"},
		Right: &QP.Literal{Value: int64(0)},
	}
	row := map[string]interface{}{"qty": int64(-1)}
	if svctx.CheckPasses(expr, row) {
		t.Error("CheckPasses(qty > 0) should be false for negative qty")
	}
}
