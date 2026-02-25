package util

import (
	"fmt"
	"reflect"
)

// Assert panics with a formatted message if the condition is false.
// This is used to catch programming errors and prevent hangs or undefined behavior.
// Usage: util.Assert(len(data) > 0, "data must not be empty")
func Assert(condition bool, format string, args ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("Assertion failed: "+format, args...))
	}
}

// Assertf is an alias for Assert for convenience
func Assertf(condition bool, format string, args ...interface{}) {
	Assert(condition, format, args...)
}

// AssertNotNil panics if the value is nil (including typed nils like (*int)(nil))
func AssertNotNil(value interface{}, name string) {
	if value == nil {
		panic(fmt.Sprintf("Assertion failed: %s must not be nil", name))
	}
	// Check for typed nil using reflection
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		panic(fmt.Sprintf("Assertion failed: %s must not be nil", name))
	}
}

// AssertTrue panics if the condition is false
func AssertTrue(condition bool, message string) {
	if !condition {
		panic(fmt.Sprintf("Assertion failed: %s", message))
	}
}

// AssertFalse panics if the condition is true
func AssertFalse(condition bool, message string) {
	if condition {
		panic(fmt.Sprintf("Assertion failed: %s", message))
	}
}
