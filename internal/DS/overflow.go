// Package DS - minimal OverflowManager for C++ wrapper compatibility
package DS

// OverflowManager handles overflow page operations.
// This is a minimal stub for C++ wrapper compatibility.
// The actual overflow handling is done in C++.
type OverflowManager struct {
	pm PageManagerInterface
}

// NewOverflowManager creates a new overflow manager.
func NewOverflowManager(pm PageManagerInterface) *OverflowManager {
	return &OverflowManager{pm: pm}
}
