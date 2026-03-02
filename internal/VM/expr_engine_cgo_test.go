package VM

import (
	"testing"
)

// TestCExprEngine_IntOp tests integer arithmetic operations.
func TestCExprEngine_IntOp(t *testing.T) {
	e := NewCExprEngine()
	if e == nil {
		t.Fatal("NewCExprEngine returned nil")
	}

	cases := []struct {
		op   int
		a, b int64
		want int64
	}{
		{ExprOpAdd, 10, 32, 42},
		{ExprOpSub, 50, 8, 42},
		{ExprOpMul, 6, 7, 42},
		{ExprOpDiv, 84, 2, 42},
		{ExprOpMod, 127, 85, 42},
	}
	for _, tc := range cases {
		got := e.EvalIntOp(tc.op, tc.a, tc.b)
		if got != tc.want {
			t.Errorf("EvalIntOp(%d, %d, %d) = %d; want %d", tc.op, tc.a, tc.b, got, tc.want)
		}
	}
}

// TestCExprEngine_FloatOp tests float arithmetic operations.
func TestCExprEngine_FloatOp(t *testing.T) {
	e := NewCExprEngine()
	cases := []struct {
		op   int
		a, b float64
		want float64
	}{
		{ExprOpAdd, 21.0, 21.0, 42.0},
		{ExprOpSub, 50.0, 8.0, 42.0},
		{ExprOpMul, 6.0, 7.0, 42.0},
		{ExprOpDiv, 84.0, 2.0, 42.0},
	}
	for _, tc := range cases {
		got := e.EvalFloatOp(tc.op, tc.a, tc.b)
		if got != tc.want {
			t.Errorf("EvalFloatOp(%d, %f, %f) = %f; want %f", tc.op, tc.a, tc.b, got, tc.want)
		}
	}
}

// TestCExprEngine_Compare tests integer comparison operations.
func TestCExprEngine_Compare(t *testing.T) {
	e := NewCExprEngine()
	if !e.EvalCompare(ExprOpEq, 42, 42) {
		t.Error("42 == 42 should be true")
	}
	if e.EvalCompare(ExprOpEq, 42, 43) {
		t.Error("42 == 43 should be false")
	}
	if !e.EvalCompare(ExprOpLt, 1, 2) {
		t.Error("1 < 2 should be true")
	}
	if !e.EvalCompare(ExprOpGe, 5, 5) {
		t.Error("5 >= 5 should be true")
	}
}

// TestCExprEngine_Logic tests logical operations.
func TestCExprEngine_Logic(t *testing.T) {
	e := NewCExprEngine()
	if !e.EvalLogic(ExprOpAnd, true, true) {
		t.Error("true AND true should be true")
	}
	if e.EvalLogic(ExprOpAnd, true, false) {
		t.Error("true AND false should be false")
	}
	if !e.EvalLogic(ExprOpOr, false, true) {
		t.Error("false OR true should be true")
	}
	if e.EvalLogic(ExprOpOr, false, false) {
		t.Error("false OR false should be false")
	}
}

// TestCExprEngine_DivByZero tests that division by zero returns 0.
func TestCExprEngine_DivByZero(t *testing.T) {
	e := NewCExprEngine()
	got := e.EvalIntOp(ExprOpDiv, 100, 0)
	if got != 0 {
		t.Errorf("100 / 0 should return 0, got %d", got)
	}
}
