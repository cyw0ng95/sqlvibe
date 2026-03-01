package context_test

import (
	"testing"

	svctx "github.com/cyw0ng95/sqlvibe/pkg/sqlvibe/context"
)

func TestAnyValueAcc_FirstNonNull(t *testing.T) {
	var acc svctx.AnyValueAcc
	acc.Accumulate(nil)
	acc.Accumulate(int64(42))
	acc.Accumulate(int64(99))
	result := acc.Result()
	if result != int64(42) {
		t.Errorf("AnyValueAcc.Result() = %v, want 42", result)
	}
}

func TestAnyValueAcc_AllNull(t *testing.T) {
	var acc svctx.AnyValueAcc
	acc.Accumulate(nil)
	acc.Accumulate(nil)
	if acc.Result() != nil {
		t.Error("AnyValueAcc.Result() should be nil when all values are NULL")
	}
}

func TestAnyValueAcc_NoValues(t *testing.T) {
	var acc svctx.AnyValueAcc
	if acc.Result() != nil {
		t.Error("AnyValueAcc.Result() should be nil when no values accumulated")
	}
}

func TestModeAcc_SingleMode(t *testing.T) {
	acc := svctx.NewModeAcc()
	acc.Accumulate("a")
	acc.Accumulate("b")
	acc.Accumulate("a")
	acc.Accumulate("a")
	result := acc.Result()
	if result != "a" {
		t.Errorf("ModeAcc.Result() = %v, want 'a'", result)
	}
}

func TestModeAcc_Tie_FirstWins(t *testing.T) {
	acc := svctx.NewModeAcc()
	acc.Accumulate("x")
	acc.Accumulate("y")
	acc.Accumulate("x")
	acc.Accumulate("y")
	// tie: x appeared first, should win
	result := acc.Result()
	if result != "x" {
		t.Errorf("ModeAcc.Result() = %v, want 'x' (first-seen tie-breaking)", result)
	}
}

func TestModeAcc_NullIgnored(t *testing.T) {
	acc := svctx.NewModeAcc()
	acc.Accumulate(nil)
	acc.Accumulate(int64(7))
	acc.Accumulate(int64(7))
	result := acc.Result()
	if result != int64(7) {
		t.Errorf("ModeAcc.Result() = %v, want 7", result)
	}
}

func TestModeAcc_Empty(t *testing.T) {
	acc := svctx.NewModeAcc()
	if acc.Result() != nil {
		t.Error("ModeAcc.Result() should be nil when empty")
	}
}
