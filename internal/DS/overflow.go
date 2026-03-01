package DS

import (
	"github.com/cyw0ng95/sqlvibe/internal/SF/util"
)

// Overflow page format:
// - First 4 bytes: Next overflow page number (0 if last)
// - Remaining bytes: Payload data

const (
	OverflowPageHeaderSize = 4
)

// OverflowManager handles overflow page operations
type OverflowManager struct {
	pm *PageManager
}

// NewOverflowManager creates a new overflow manager
func NewOverflowManager(pm *PageManager) *OverflowManager {
	util.AssertNotNil(pm, "PageManager")
	return &OverflowManager{pm: pm}
}
