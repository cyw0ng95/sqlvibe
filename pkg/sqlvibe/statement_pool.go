package sqlvibe

import "sync"

const defaultPoolSize = 100

// StatementPool manages a pool of prepared statements with LRU eviction.
type StatementPool struct {
	mu      sync.RWMutex
	stmts   map[string]*Statement
	lru     []string
	maxSize int
	db      *Database
}

// NewStatementPool creates a StatementPool for db with the given maximum size.
// If maxSize <= 0, defaultPoolSize is used.
func NewStatementPool(db *Database, maxSize int) *StatementPool {
	if maxSize <= 0 {
		maxSize = defaultPoolSize
	}
	return &StatementPool{
		stmts:   make(map[string]*Statement, maxSize),
		lru:     make([]string, 0, maxSize),
		maxSize: maxSize,
		db:      db,
	}
}

// Get retrieves a prepared statement from the pool or compiles a new one.
func (sp *StatementPool) Get(sql string) (*Statement, error) {
	sp.mu.RLock()
	stmt, ok := sp.stmts[sql]
	sp.mu.RUnlock()
	if ok {
		sp.touch(sql)
		return stmt, nil
	}

	sp.mu.Lock()
	defer sp.mu.Unlock()
	if stmt, ok = sp.stmts[sql]; ok {
		sp.touchLocked(sql)
		return stmt, nil
	}
	if len(sp.stmts) >= sp.maxSize {
		sp.evictLRU()
	}
	stmt, err := sp.db.Prepare(sql)
	if err != nil {
		return nil, err
	}
	sp.stmts[sql] = stmt
	sp.lru = append(sp.lru, sql)
	return stmt, nil
}

// Clear removes all cached statements from the pool.
func (sp *StatementPool) Clear() {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	for _, stmt := range sp.stmts {
		stmt.Close()
	}
	sp.stmts = make(map[string]*Statement, sp.maxSize)
	sp.lru = sp.lru[:0]
}

// Len returns the number of cached statements.
func (sp *StatementPool) Len() int {
	sp.mu.RLock()
	defer sp.mu.RUnlock()
	return len(sp.stmts)
}

func (sp *StatementPool) touch(sql string) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	sp.touchLocked(sql)
}

func (sp *StatementPool) touchLocked(sql string) {
	for i, s := range sp.lru {
		if s == sql {
			sp.lru = append(sp.lru[:i], sp.lru[i+1:]...)
			sp.lru = append(sp.lru, sql)
			return
		}
	}
}

func (sp *StatementPool) evictLRU() {
	if len(sp.lru) == 0 {
		return
	}
	oldest := sp.lru[0]
	if stmt, ok := sp.stmts[oldest]; ok {
		stmt.Close()
	}
	delete(sp.stmts, oldest)
	sp.lru = sp.lru[1:]
}
