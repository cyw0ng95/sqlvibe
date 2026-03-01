//go:build SVDB_ENABLE_CGO_VM
// +build SVDB_ENABLE_CGO_VM

package sqlvibe

/*
#cgo LDFLAGS: -L${SRCDIR}/cgo/../../.build/cmake/lib -lsvdb_vm_phase2
#cgo CFLAGS: -I${SRCDIR}/cgo
#include "hash_join.h"
#include <stdlib.h>
*/
import "C"

// execHashJoinCGO executes a hash join using CGO acceleration
// Returns (rows, columns, true) when CGO hash join was used, or (nil, nil, false) when fallback needed
// NOTE: Currently disabled - CGO implementation needs type preservation fixes
func execHashJoinCGO(
	leftData []map[string]interface{},
	rightData []map[string]interface{},
	leftCols []string,
	rightCols []string,
	leftJoinKey string,
	rightJoinKey string,
) ([][]interface{}, []string, bool) {
	// CGO implementation temporarily disabled - type preservation needs work
	// Falls back to pure Go implementation
	return nil, nil, false
}
