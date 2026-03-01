//go:build !SVDB_ENABLE_CGO_VM
// +build !SVDB_ENABLE_CGO_VM

package sqlvibe

// execHashJoinCGO is a stub for pure Go builds - returns false to use Go implementation
func execHashJoinCGO(
	leftData []map[string]interface{},
	rightData []map[string]interface{},
	leftCols []string,
	rightCols []string,
	leftJoinKey string,
	rightJoinKey string,
) ([][]interface{}, []string, bool) {
	// Pure Go build - use Go implementation
	return nil, nil, false
}
