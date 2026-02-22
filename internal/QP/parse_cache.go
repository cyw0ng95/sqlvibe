package QP

import (
	"container/list"
	"sync"
)

type lruParseCache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	list     *list.List
}

type lruParseCacheEntry struct {
	key  string
	stmt interface{} // the parsed statement
}

func newLRUParseCache(capacity int) *lruParseCache {
	return &lruParseCache{
		capacity: capacity,
		items:    make(map[string]*list.Element, capacity),
		list:     list.New(),
	}
}

func (c *lruParseCache) Get(key string) (interface{}, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.list.MoveToFront(el)
		return el.Value.(*lruParseCacheEntry).stmt, true
	}
	return nil, false
}

func (c *lruParseCache) Set(key string, stmt interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.list.MoveToFront(el)
		el.Value.(*lruParseCacheEntry).stmt = stmt
		return
	}
	if c.list.Len() >= c.capacity {
		back := c.list.Back()
		if back != nil {
			c.list.Remove(back)
			delete(c.items, back.Value.(*lruParseCacheEntry).key)
		}
	}
	el := c.list.PushFront(&lruParseCacheEntry{key: key, stmt: stmt})
	c.items[key] = el
}

var parseCache = newLRUParseCache(1000)

// ParseCached parses a SQL string, returning a cached result when available.
// It uses NormalizeQuery as the cache key for structural deduplication.
func ParseCached(sql string) (ASTNode, error) {
	key := NormalizeQuery(sql)
	if cached, ok := parseCache.Get(key); ok {
		return cached.(ASTNode), nil
	}
	tokens, err := NewTokenizer(sql).Tokenize()
	if err != nil {
		return nil, err
	}
	stmt, err := NewParser(tokens).Parse()
	if err != nil {
		return nil, err
	}
	parseCache.Set(key, stmt)
	return stmt, nil
}
