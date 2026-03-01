package util

import (
	"testing"
)

func TestAssert_Pass(t *testing.T) {
	// Should not panic
	Assert(true, "this should pass")
	Assert(1 == 1, "math works")
	Assert(len("test") == 4, "string length is %d", 4)
}

func TestAssert_Fail(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("Assert should have panicked")
		}
	}()
	Assert(false, "this should fail")
}

func TestAssertf_Fail(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Error("Assertf should have panicked")
		}
		if msg, ok := r.(string); ok {
			if msg != "Assertion failed: value 5 is not equal to 10" {
				t.Errorf("Unexpected panic message: %s", msg)
			}
		}
	}()
	Assertf(5 == 10, "value %d is not equal to %d", 5, 10)
}

func TestAssertNotNil_Pass(t *testing.T) {
	s := "test"
	AssertNotNil(s, "string")
	AssertNotNil(&s, "pointer")
}

func TestAssertNotNil_Fail(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("AssertNotNil should have panicked")
		}
	}()
	var ptr *string
	AssertNotNil(ptr, "pointer")
}

func TestAssertTrue_Pass(t *testing.T) {
	AssertTrue(true, "should pass")
	AssertTrue(1 == 1, "should pass")
}

func TestAssertTrue_Fail(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("AssertTrue should have panicked")
		}
	}()
	AssertTrue(false, "this is false")
}

func TestAssertFalse_Pass(t *testing.T) {
	AssertFalse(false, "should pass")
	AssertFalse(1 == 2, "should pass")
}

func TestAssertFalse_Fail(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("AssertFalse should have panicked")
		}
	}()
	AssertFalse(true, "this is true")
}

func TestAssertNotNil_NilInterface(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("AssertNotNil should have panicked for nil interface")
		}
	}()
	AssertNotNil(nil, "nilval")
}

// --- pool.go -----------------------------------------------------------------

func TestGetPutByteBuffer(t *testing.T) {
	buf := GetByteBuffer()
	if buf == nil {
		t.Fatal("expected non-nil buffer")
	}
	if len(*buf) == 0 {
		t.Error("expected non-empty buffer")
	}
	// Return to pool
	PutByteBuffer(buf)
	// nil should not panic
	PutByteBuffer(nil)
}

func TestGetPutInterfaceSlice(t *testing.T) {
	s := GetInterfaceSlice()
	if s == nil {
		t.Fatal("expected non-nil slice")
	}
	if len(*s) != 0 {
		t.Error("expected empty slice (reset to length 0)")
	}
	*s = append(*s, "a", "b", "c")
	PutInterfaceSlice(s)

	// Next Get should have length 0 (reset)
	s2 := GetInterfaceSlice()
	if len(*s2) != 0 {
		t.Errorf("expected reset slice, got length %d", len(*s2))
	}
	PutInterfaceSlice(s2)
	// nil should not panic
	PutInterfaceSlice(nil)
}
