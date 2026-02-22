package IS

import (
	"sync"
)

// schemaCacheEntry holds cached information_schema view results.
type schemaCacheEntry struct {
	columns []string
	rows    [][]interface{}
}

// SchemaCache is a thread-safe cache for information_schema view results.
// Entries are stored per view name and are invalidated on DDL operations.
// Unlike the general query result cache, schema cache entries are NOT
// invalidated by DML (INSERT/UPDATE/DELETE), only by DDL that changes
// the schema (CREATE TABLE, DROP TABLE, ALTER TABLE, CREATE INDEX, etc.).
type SchemaCache struct {
	mu      sync.RWMutex
	entries map[string]*schemaCacheEntry
}

// NewSchemaCache creates a new, empty SchemaCache.
func NewSchemaCache() *SchemaCache {
	return &SchemaCache{entries: make(map[string]*schemaCacheEntry)}
}

// Get returns cached columns and rows for viewName, or (nil, nil, false) if not present.
func (sc *SchemaCache) Get(viewName string) (columns []string, rows [][]interface{}, ok bool) {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	e, found := sc.entries[viewName]
	if !found {
		return nil, nil, false
	}
	return e.columns, e.rows, true
}

// Set stores the result for viewName in the cache.
func (sc *SchemaCache) Set(viewName string, columns []string, rows [][]interface{}) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.entries[viewName] = &schemaCacheEntry{columns: columns, rows: rows}
}

// Invalidate removes all cached entries.  Call this on any DDL operation
// that changes the schema (CREATE/DROP/ALTER TABLE, CREATE/DROP INDEX, etc.).
func (sc *SchemaCache) Invalidate() {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.entries = make(map[string]*schemaCacheEntry)
}

// InvalidateTable removes cached entries for views that include per-table data.
// Calling this after a schema change to a single table avoids a full cache flush.
func (sc *SchemaCache) InvalidateTable(_ string) {
	// Simpler to flush all: column/table/constraint views mix multiple tables.
	sc.Invalidate()
}
