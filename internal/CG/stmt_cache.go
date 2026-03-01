package CG

import (
	"sync"
	"time"
)

// ParsedStatement holds a pre-parsed statement ready for compilation.
// Caching this avoids re-tokenising and re-parsing the same SQL string.
type ParsedStatement struct {
	SQL       string      // Original SQL text
	Statement interface{} // *QP.SelectStmt, *QP.InsertStmt, etc.
	ParsedAt  time.Time   // When the entry was created
}

// StmtCache is a thread-safe LRU-style cache for parsed SQL statements.
// It avoids the tokenisation + parsing overhead for repeated identical queries.
type StmtCache struct {
	mu    sync.RWMutex
	data  map[string]*stmtEntry
	limit int
}

type stmtEntry struct {
	stmt      *ParsedStatement
	createdAt time.Time
}

// NewStmtCache creates a StmtCache that holds at most limit entries.
// A zero or negative limit disables eviction (unbounded cache).
func NewStmtCache(limit int) *StmtCache {
	return &StmtCache{
		data:  make(map[string]*stmtEntry),
		limit: limit,
	}
}

// Get returns the cached ParsedStatement for sql and true when an entry exists.
func (sc *StmtCache) Get(sql string) (*ParsedStatement, bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()

	if e, ok := sc.data[sql]; ok {
		return e.stmt, true
	}
	return nil, false
}

// Put stores stmt under sql.  When the cache is full, the oldest entry is evicted.
func (sc *StmtCache) Put(sql string, stmt *ParsedStatement) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.limit > 0 && len(sc.data) >= sc.limit && len(sc.data) > 0 {
		sc.evictOldest()
	}
	sc.data[sql] = &stmtEntry{
		stmt:      stmt,
		createdAt: time.Now(),
	}
}

// Invalidate removes all entries from the cache.
func (sc *StmtCache) Invalidate() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.data = make(map[string]*stmtEntry)
}

// Len returns the number of entries currently in the cache.
func (sc *StmtCache) Len() int {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return len(sc.data)
}

// evictOldest removes the entry with the earliest createdAt timestamp.
// Caller must hold mu.Lock().
func (sc *StmtCache) evictOldest() {
	var oldestKey string
	var oldestTime time.Time
	first := true
	for k, v := range sc.data {
		if first || v.createdAt.Before(oldestTime) {
			oldestKey = k
			oldestTime = v.createdAt
			first = false
		}
	}
	if !first {
		delete(sc.data, oldestKey)
	}
}
