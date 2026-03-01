package context_test

import (
	"testing"

	svctx "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/context"
)

func TestIsTruthy_Nil(t *testing.T) {
	if svctx.IsTruthy(nil) {
		t.Error("IsTruthy(nil) should be false")
	}
}

func TestIsTruthy_Zero(t *testing.T) {
	if svctx.IsTruthy(int64(0)) {
		t.Error("IsTruthy(0) should be false")
	}
	if svctx.IsTruthy(float64(0)) {
		t.Error("IsTruthy(0.0) should be false")
	}
	if svctx.IsTruthy("") {
		t.Error("IsTruthy(\"\") should be false")
	}
	if svctx.IsTruthy(false) {
		t.Error("IsTruthy(false) should be false")
	}
}

func TestIsTruthy_NonZero(t *testing.T) {
	if !svctx.IsTruthy(int64(1)) {
		t.Error("IsTruthy(1) should be true")
	}
	if !svctx.IsTruthy(float64(0.5)) {
		t.Error("IsTruthy(0.5) should be true")
	}
	if !svctx.IsTruthy("hello") {
		t.Error(`IsTruthy("hello") should be true`)
	}
	if !svctx.IsTruthy(true) {
		t.Error("IsTruthy(true) should be true")
	}
}

func TestAddVals_Int(t *testing.T) {
	result := svctx.AddVals(int64(3), int64(4))
	if result != int64(7) {
		t.Errorf("AddVals(3,4) = %v, want 7", result)
	}
}

func TestAddVals_Float(t *testing.T) {
	result := svctx.AddVals(int64(3), float64(1.5))
	if result != float64(4.5) {
		t.Errorf("AddVals(3,1.5) = %v, want 4.5", result)
	}
}

func TestSubVals(t *testing.T) {
	result := svctx.SubVals(int64(10), int64(3))
	if result != int64(7) {
		t.Errorf("SubVals(10,3) = %v, want 7", result)
	}
}

func TestMulVals(t *testing.T) {
	result := svctx.MulVals(int64(3), int64(4))
	if result != int64(12) {
		t.Errorf("MulVals(3,4) = %v, want 12", result)
	}
}

func TestDivVals_Normal(t *testing.T) {
	result := svctx.DivVals(int64(10), int64(2))
	if result != int64(5) {
		t.Errorf("DivVals(10,2) = %v, want 5", result)
	}
}

func TestDivVals_DivisionByZero(t *testing.T) {
	result := svctx.DivVals(int64(10), int64(0))
	if result != nil {
		t.Errorf("DivVals(10,0) = %v, want nil", result)
	}
}

func TestArith_Nil(t *testing.T) {
	if svctx.AddVals(nil, int64(1)) != nil {
		t.Error("AddVals(nil,1) should be nil")
	}
	if svctx.SubVals(nil, int64(1)) != nil {
		t.Error("SubVals(nil,1) should be nil")
	}
	if svctx.MulVals(nil, int64(1)) != nil {
		t.Error("MulVals(nil,1) should be nil")
	}
	if svctx.DivVals(nil, int64(1)) != nil {
		t.Error("DivVals(nil,1) should be nil")
	}
}
