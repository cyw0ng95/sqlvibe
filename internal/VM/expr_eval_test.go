package VM

import (
	"testing"
)

func TestExprBytecode_New(t *testing.T) {
	eb := NewExprBytecode()
	if eb == nil {
		t.Error("NewExprBytecode should not return nil")
	}
	if eb.ops == nil {
		t.Error("ops should be initialized")
	}
	if eb.args == nil {
		t.Error("args should be initialized")
	}
	if eb.consts == nil {
		t.Error("consts should be initialized")
	}
}

func TestExprBytecode_Emit(t *testing.T) {
	eb := NewExprBytecode()

	eb.Emit(EOpLoadConst, 0)
	eb.Emit(EOpLoadColumn, 1)
	eb.Emit(EOpAdd)

	if len(eb.ops) != 3 {
		t.Errorf("Expected 3 ops, got %d", len(eb.ops))
	}
	if eb.ops[0] != EOpLoadConst {
		t.Errorf("First op should be EOpLoadConst, got %v", eb.ops[0])
	}
}

func TestExprBytecode_AddConst(t *testing.T) {
	eb := NewExprBytecode()

	idx1 := eb.AddConst(int64(42))
	if idx1 != 0 {
		t.Errorf("First const index should be 0, got %d", idx1)
	}

	idx2 := eb.AddConst("hello")
	if idx2 != 1 {
		t.Errorf("Second const index should be 1, got %d", idx2)
	}

	idx3 := eb.AddConst(nil)
	if idx3 != 2 {
		t.Errorf("Third const index should be 2, got %d", idx3)
	}

	if len(eb.consts) != 3 {
		t.Errorf("Expected 3 consts, got %d", len(eb.consts))
	}
}

func TestExprBytecode_Ops(t *testing.T) {
	eb := NewExprBytecode()
	eb.Emit(EOpAdd)

	ops := eb.Ops()
	if len(ops) != 1 {
		t.Errorf("Expected 1 op, got %d", len(ops))
	}
}

func TestExprBytecode_Eval(t *testing.T) {
	eb := NewExprBytecode()

	ci := eb.AddConst(int64(42))
	eb.Emit(EOpLoadConst, ci)

	result := eb.Eval(nil)
	if result != int64(42) {
		t.Errorf("Expected 42, got %v", result)
	}
}

func TestExprBytecode_Eval_Add(t *testing.T) {
	eb := NewExprBytecode()

	c1 := eb.AddConst(int64(10))
	c2 := eb.AddConst(int64(20))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpLoadConst, c2)
	eb.Emit(EOpAdd)

	result := eb.Eval(nil)
	if result != int64(30) {
		t.Errorf("Expected 30, got %v", result)
	}
}

func TestExprBytecode_Eval_Column(t *testing.T) {
	eb := NewExprBytecode()

	eb.Emit(EOpLoadColumn, 1)

	row := []interface{}{int64(1), "test", int64(3)}
	result := eb.Eval(row)
	if result != "test" {
		t.Errorf("Expected 'test', got %v", result)
	}
}

func TestExprBytecode_Eval_Sub(t *testing.T) {
	eb := NewExprBytecode()
	c1 := eb.AddConst(int64(10))
	c2 := eb.AddConst(int64(3))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpLoadConst, c2)
	eb.Emit(EOpSub)

	result := eb.Eval(nil)
	if result != int64(7) {
		t.Errorf("Sub: Expected 7, got %v", result)
	}
}

func TestExprBytecode_Eval_Mul(t *testing.T) {
	eb := NewExprBytecode()
	c1 := eb.AddConst(int64(6))
	c2 := eb.AddConst(int64(7))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpLoadConst, c2)
	eb.Emit(EOpMul)
	result := eb.Eval(nil)
	if result != int64(42) {
		t.Errorf("Mul: Expected 42, got %v", result)
	}
}

func TestExprBytecode_Eval_Div(t *testing.T) {
	eb := NewExprBytecode()
	c1 := eb.AddConst(int64(100))
	c2 := eb.AddConst(int64(4))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpLoadConst, c2)
	eb.Emit(EOpDiv)
	result := eb.Eval(nil)
	_ = result // Just verify no panic
}

func TestExprBytecode_Eval_Mod(t *testing.T) {
	eb := NewExprBytecode()
	c1 := eb.AddConst(int64(42))
	c2 := eb.AddConst(int64(5))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpLoadConst, c2)
	eb.Emit(EOpMod)
	result := eb.Eval(nil)
	if result != int64(2) {
		t.Errorf("Mod: Expected 2, got %v", result)
	}
}

func TestExprBytecode_Eval_Eq(t *testing.T) {
	eb := NewExprBytecode()
	c1 := eb.AddConst(int64(5))
	c2 := eb.AddConst(int64(5))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpLoadConst, c2)
	eb.Emit(EOpEq)
	result := eb.Eval(nil)
	if result != int64(1) {
		t.Errorf("Eq: Expected 1, got %v", result)
	}
}

func TestExprBytecode_Eval_Ne(t *testing.T) {
	eb := NewExprBytecode()
	c1 := eb.AddConst(int64(5))
	c2 := eb.AddConst(int64(3))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpLoadConst, c2)
	eb.Emit(EOpNe)
	result := eb.Eval(nil)
	if result != int64(1) {
		t.Errorf("Ne: Expected 1, got %v", result)
	}
}

func TestExprBytecode_Eval_Lt(t *testing.T) {
	eb := NewExprBytecode()
	c1 := eb.AddConst(int64(3))
	c2 := eb.AddConst(int64(5))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpLoadConst, c2)
	eb.Emit(EOpLt)
	result := eb.Eval(nil)
	if result != int64(1) {
		t.Errorf("Lt: Expected 1, got %v", result)
	}
}

func TestExprBytecode_Eval_Le(t *testing.T) {
	eb := NewExprBytecode()
	c1 := eb.AddConst(int64(5))
	c2 := eb.AddConst(int64(5))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpLoadConst, c2)
	eb.Emit(EOpLe)
	result := eb.Eval(nil)
	if result != int64(1) {
		t.Errorf("Le: Expected 1, got %v", result)
	}
}

func TestExprBytecode_Eval_Gt(t *testing.T) {
	eb := NewExprBytecode()
	c1 := eb.AddConst(int64(10))
	c2 := eb.AddConst(int64(5))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpLoadConst, c2)
	eb.Emit(EOpGt)
	result := eb.Eval(nil)
	if result != int64(1) {
		t.Errorf("Gt: Expected 1, got %v", result)
	}
}

func TestExprBytecode_Eval_Ge(t *testing.T) {
	eb := NewExprBytecode()
	c1 := eb.AddConst(int64(5))
	c2 := eb.AddConst(int64(5))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpLoadConst, c2)
	eb.Emit(EOpGe)
	result := eb.Eval(nil)
	if result != int64(1) {
		t.Errorf("Ge: Expected 1, got %v", result)
	}
}

func TestExprBytecode_Eval_Negate(t *testing.T) {
	eb := NewExprBytecode()
	c1 := eb.AddConst(int64(42))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpNeg)
	result := eb.Eval(nil)
	if result != int64(-42) {
		t.Errorf("Expected -42, got %v", result)
	}
}

func TestExprBytecode_Eval_Float(t *testing.T) {
	eb := NewExprBytecode()
	c1 := eb.AddConst(float64(1.5))
	c2 := eb.AddConst(float64(2.5))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpLoadConst, c2)
	eb.Emit(EOpAdd)
	result := eb.Eval(nil)
	if result != float64(4.0) {
		t.Errorf("Expected 4.0, got %v", result)
	}
}

func TestExprBytecode_Eval_Complex(t *testing.T) {
	eb := NewExprBytecode()
	c1 := eb.AddConst(int64(2))
	c2 := eb.AddConst(int64(3))
	c3 := eb.AddConst(int64(4))
	eb.Emit(EOpLoadConst, c1)
	eb.Emit(EOpLoadConst, c2)
	eb.Emit(EOpAdd)
	eb.Emit(EOpLoadConst, c3)
	eb.Emit(EOpMul)
	result := eb.Eval(nil)
	if result != int64(20) {
		t.Errorf("Expected 20, got %v", result)
	}
}

func TestExprPop(t *testing.T) {
	stack := []interface{}{int64(1), int64(2), int64(3)}
	val := exprPop(&stack)
	if val != int64(3) {
		t.Errorf("exprPop = 3, got %v", val)
	}
	if len(stack) != 2 {
		t.Errorf("Stack length = 2, got %d", len(stack))
	}
}

func TestExprCompare(t *testing.T) {
	tests := []struct {
		a, b interface{}
		want int
	}{
		{int64(1), int64(1), 0},
		{int64(1), int64(2), -1},
		{int64(2), int64(1), 1},
		{float64(1.0), float64(1.0), 0},
		{float64(1.0), float64(2.0), -1},
		{"a", "a", 0},
		{"a", "b", -1},
		{"b", "a", 1},
		{nil, nil, 0},
	}

	for _, tt := range tests {
		got := exprCompare(tt.a, tt.b)
		if got != tt.want {
			t.Errorf("exprCompare(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestExprNumericOp(t *testing.T) {
	tests := []struct {
		a    interface{}
		b    interface{}
		op   int
		want interface{}
	}{
		{int64(10), int64(3), 0, int64(13)},
		{int64(10), int64(3), 1, int64(7)},
		{int64(10), int64(3), 2, int64(30)},
		{int64(10), int64(3), 4, int64(1)},
		{float64(10), float64(3), 0, float64(13)},
	}

	for _, tt := range tests {
		got := exprNumericOp(tt.a, tt.b, tt.op)
		if got != tt.want {
			t.Errorf("exprNumericOp(%v, %v, %d) = %v, want %v", tt.a, tt.b, tt.op, got, tt.want)
		}
	}
}

func TestExprNumericOp_Invalid(t *testing.T) {
	result := exprNumericOp("invalid", "args", 0)
	if result != nil {
		t.Errorf("exprNumericOp with invalid args = %v, want nil", result)
	}
}

func TestExprNumericOp_DivideByZero(t *testing.T) {
	result := exprNumericOp(int64(10), int64(0), 3)
	if result != nil {
		t.Errorf("Divide by zero = %v, want nil", result)
	}
}

func TestExprToFloat(t *testing.T) {
	tests := []struct {
		input  interface{}
		want   float64
		wantOk bool
	}{
		{int64(42), 42.0, true},
		{int(42), 42.0, true},
		{float64(3.14), 3.14, true},
		{nil, 0, false},
		{"test", 0, false},
	}

	for _, tt := range tests {
		got, ok := exprToFloat(tt.input)
		if ok != tt.wantOk {
			t.Errorf("exprToFloat(%v) ok = %v, want %v", tt.input, ok, tt.wantOk)
		}
		if ok && got != tt.want {
			t.Errorf("exprToFloat(%v) = %v, want %v", tt.input, got, tt.want)
		}
	}
}
