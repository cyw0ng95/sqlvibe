package sqlvibe

import "sync"

// rowPool is a pool of reusable []interface{} slices for temporary row buffers.
// Callers must clear the slice before putting it back.
var rowPool = sync.Pool{
	New: func() interface{} {
		s := make([]interface{}, 0, 64)
		return &s
	},
}

// mapPool is a pool of reusable map[string]interface{} for temporary row maps.
// Callers must clear the map before putting it back.
var mapPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]interface{}, 16)
	},
}

// schemaMapPool is a pool of reusable map[string]int for schema lookups (column index maps).
// Callers must clear the map before putting it back.
var schemaMapPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]int, 16)
	},
}

// colSetPool is a pool of reusable map[string]bool for column-name sets.
// Callers must clear the map before putting it back.
var colSetPool = sync.Pool{
	New: func() interface{} {
		return make(map[string]bool, 16)
	},
}
