package vm_test

import (
	"testing"

	"github.com/cyw0ng95/sqlvibe/internal/QP"
	svvm "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/vm"
)

func TestExprToSQL_Nil(t *testing.T) {
	s := svvm.ExprToSQL(nil)
	if s != "NULL" {
		t.Errorf("expected NULL, got %q", s)
	}
}

func TestExprToSQL_Literal_Int(t *testing.T) {
	s := svvm.ExprToSQL(&QP.Literal{Value: int64(7)})
	if s != "7" {
		t.Errorf("expected '7', got %q", s)
	}
}

func TestExprToSQL_Literal_String(t *testing.T) {
	s := svvm.ExprToSQL(&QP.Literal{Value: "hi"})
	if s != "'hi'" {
		t.Errorf("expected \"'hi'\", got %q", s)
	}
}

func TestExprToSQL_ColumnRef(t *testing.T) {
	s := svvm.ExprToSQL(&QP.ColumnRef{Name: "age"})
	if s != "age" {
		t.Errorf("expected 'age', got %q", s)
	}
}

func TestExprToSQL_ColumnRef_Qualified(t *testing.T) {
	s := svvm.ExprToSQL(&QP.ColumnRef{Table: "t", Name: "age"})
	if s != "t.age" {
		t.Errorf("expected 't.age', got %q", s)
	}
}

func TestExprToSQL_BinaryExpr_Add(t *testing.T) {
	expr := &QP.BinaryExpr{
		Op:    QP.TokenPlus,
		Left:  &QP.Literal{Value: int64(1)},
		Right: &QP.Literal{Value: int64(2)},
	}
	s := svvm.ExprToSQL(expr)
	if s != "(1 + 2)" {
		t.Errorf("expected '(1 + 2)', got %q", s)
	}
}

func TestExprToSQL_UnaryMinus(t *testing.T) {
	expr := &QP.UnaryExpr{Op: QP.TokenMinus, Expr: &QP.Literal{Value: int64(5)}}
	s := svvm.ExprToSQL(expr)
	if s != "-5" {
		t.Errorf("expected '-5', got %q", s)
	}
}

func TestExprToSQL_FuncCall(t *testing.T) {
	expr := &QP.FuncCall{
		Name: "upper",
		Args: []QP.Expr{&QP.ColumnRef{Name: "name"}},
	}
	s := svvm.ExprToSQL(expr)
	if s != "upper(name)" {
		t.Errorf("expected 'upper(name)', got %q", s)
	}
}
