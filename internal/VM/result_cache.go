package VM

import (
	"sync"
	"time"
)

// ResultCache is a thread-safe TTL-based cache for VM query results.
// It stores the most recent result rows for a given uint64 cache key so that
// repeated identical read-only queries can be served without re-executing the
// full VM program.
type ResultCache struct {
	mu    sync.RWMutex
	data  map[uint64]*resultCacheEntry
	limit int
	ttl   time.Duration
}

type resultCacheEntry struct {
	rows    [][]interface{}
	created time.Time
}

// NewResultCache creates a ResultCache that holds at most limit entries, each
// valid for ttl duration.  A zero or negative limit disables eviction (unbounded).
func NewResultCache(limit int, ttl time.Duration) *ResultCache {
	return &ResultCache{
		data:  make(map[uint64]*resultCacheEntry),
		limit: limit,
		ttl:   ttl,
	}
}

// Get returns the cached rows for key, and true when a valid (non-expired) entry
// exists.  Returns nil, false on cache miss or expiry.
func (rc *ResultCache) Get(key uint64) ([][]interface{}, bool) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if e, ok := rc.data[key]; ok {
		if rc.ttl <= 0 || time.Since(e.created) < rc.ttl {
			return e.rows, true
		}
	}
	return nil, false
}

// Set stores rows under key, evicting the oldest entry first when the cache is
// full and limit > 0.
func (rc *ResultCache) Set(key uint64, rows [][]interface{}) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.limit > 0 && len(rc.data) >= rc.limit {
		rc.evictOldest()
	}
	rc.data[key] = &resultCacheEntry{rows: rows, created: time.Now()}
}

// Invalidate clears all entries from the cache.
func (rc *ResultCache) Invalidate() {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	rc.data = make(map[uint64]*resultCacheEntry)
}

// evictOldest removes the single oldest (earliest created) entry.
// Caller must hold mu.Lock().
func (rc *ResultCache) evictOldest() {
	var oldestKey uint64
	var oldestTime time.Time
	first := true
	for k, e := range rc.data {
		if first || e.created.Before(oldestTime) {
			oldestKey = k
			oldestTime = e.created
			first = false
		}
	}
	if !first {
		delete(rc.data, oldestKey)
	}
}
