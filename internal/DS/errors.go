// Package DS - error types for C++ wrapper compatibility
package DS

import "errors"

// Error definitions (only define if not already in ds_v2_cgo.go)
var (
	ErrInvalidPageSize = errors.New("invalid page size")
	ErrInvalidPage     = errors.New("invalid page")
	// ErrCreateFailed, ErrReadFailed, etc. are defined in ds_v2_cgo.go
)

// ManagerIsValidPageSize checks if the page size is valid.
func ManagerIsValidPageSize(pageSize uint32) bool {
	return pageSize >= 512 && pageSize <= 65536 && (pageSize&(pageSize-1)) == 0
}
