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
