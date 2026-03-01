//go:build !SVDB_ENABLE_CGO_QP
// +build !SVDB_ENABLE_CGO_QP

package QP

// FastTokenCount returns an estimated token count for pre-allocation
// (pure Go fallback — approximation based on SQL length).
//
// The heuristic is 1 token per 4 bytes of SQL + 8 base allocation.
// Empirically, average SQL token density for typical queries is 3-5 bytes
// per token (keywords ~3-8 chars, identifiers ~4-12 chars, operators 1-2 chars).
// A factor of 4 provides a conservative over-estimate to avoid re-allocation.
func FastTokenCount(sql string) int {
	// Rough estimate: ~1 token per 4 bytes of SQL
	n := len(sql)/4 + 8
	if n < 8 {
		n = 8
	}
	return n
}
