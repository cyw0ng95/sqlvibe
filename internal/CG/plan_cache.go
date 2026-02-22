package CG

import (
	"sync"
	"sync/atomic"
	"time"

	VM "github.com/sqlvibe/sqlvibe/internal/VM"
)

// PlanCache is a thread-safe LRU-style cache for compiled VM programs.
// Caching a compiled plan avoids the parse + code-generation overhead for
// repeated identical SQL statements (e.g., parameterised queries executed in a
// loop, or re-used prepared statements).
type PlanCache struct {
	mu    sync.RWMutex
	data  map[string]*cachedPlan
	limit int
}

type cachedPlan struct {
	program   *VM.Program
	createdAt time.Time
	hits      int64 // updated atomically; no lock required for increment
}

// NewPlanCache creates a PlanCache that holds at most limit entries.
// A zero or negative limit disables eviction (unbounded cache).
func NewPlanCache(limit int) *PlanCache {
	return &PlanCache{
		data:  make(map[string]*cachedPlan),
		limit: limit,
	}
}

// Get returns the cached program for sql and true when an entry exists.
// It atomically increments the hit counter for the entry.
func (pc *PlanCache) Get(sql string) (*VM.Program, bool) {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	if plan, ok := pc.data[sql]; ok {
		atomic.AddInt64(&plan.hits, 1)
		return plan.program, true
	}
	return nil, false
}

// Put stores program under sql.  When the cache is full, the least-recently
// created entry is evicted to make room.
func (pc *PlanCache) Put(sql string, program *VM.Program) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	if pc.limit > 0 && len(pc.data) >= pc.limit && len(pc.data) > 0 {
		pc.evictOldest()
	}
	pc.data[sql] = &cachedPlan{
		program:   program,
		createdAt: time.Now(),
	}
}

// Invalidate removes all entries from the cache.
func (pc *PlanCache) Invalidate() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.data = make(map[string]*cachedPlan)
}

// evictOldest removes the entry with the earliest createdAt timestamp.
// Caller must hold mu.Lock().
func (pc *PlanCache) evictOldest() {
	var oldestKey string
	var oldestTime time.Time
	first := true
	for k, v := range pc.data {
		if first || v.createdAt.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.createdAt
			first = false
		}
	}
	if !first {
		delete(pc.data, oldestKey)
	}
}
