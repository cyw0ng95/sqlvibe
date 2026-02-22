package VM

import "sync"

// stringPool is a global pool for interning frequently repeated strings.
// Interning replaces a string value with the canonical pooled copy, so all
// identical strings share a single backing allocation and pointer comparisons
// can substitute for value comparisons in hot paths.
var stringPool sync.Map

// InternString returns the canonical pooled copy of s. If s has not been seen
// before it is stored in the pool and returned as-is. Subsequent calls with the
// same value return the original stored pointer without any allocation.
func InternString(s string) string {
	if v, ok := stringPool.Load(s); ok {
		return v.(string)
	}
	stringPool.Store(s, s)
	return s
}
