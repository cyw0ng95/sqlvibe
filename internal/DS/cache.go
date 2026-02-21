package DS

import (
	"container/list"
	"sync"
)

type lruEntry struct {
	pageNum uint32
	page    *Page
}

// Cache is an LRU page cache. Get promotes entries to most-recently-used;
// when capacity is exceeded the least-recently-used entry is evicted.
type Cache struct {
	mu       sync.Mutex
	capacity int
	list     *list.List
	items    map[uint32]*list.Element
	hits     int
	misses   int
}

// NewCache creates an LRU cache. Capacity follows the SQLite convention:
// positive values are page counts; negative values represent the cache size
// in kibibytes (e.g. -2000 means 2000 KiB). A capacity of 0 defaults to
// 2000 pages.
func NewCache(capacity int) *Cache {
	if capacity < 0 {
		// Negative: interpret magnitude as KiB; DefaultPageSize is 4096 bytes (4 KiB).
		capacity = (-capacity * 1024) / DefaultPageSize
	}
	if capacity <= 0 {
		capacity = 2000
	}
	return &Cache{
		capacity: capacity,
		list:     list.New(),
		items:    make(map[uint32]*list.Element),
	}
}

// Get returns the page for pageNum and promotes it to most-recently-used.
func (c *Cache) Get(pageNum uint32) (*Page, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[pageNum]; ok {
		c.list.MoveToFront(elem)
		c.hits++
		return elem.Value.(*lruEntry).page, true
	}
	c.misses++
	return nil, false
}

// Set inserts or updates a page in the cache, evicting the LRU entry if full.
func (c *Cache) Set(page *Page) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[page.Num]; ok {
		c.list.MoveToFront(elem)
		elem.Value.(*lruEntry).page = page
		return
	}
	if c.list.Len() >= c.capacity {
		c.evictLocked()
	}
	elem := c.list.PushFront(&lruEntry{pageNum: page.Num, page: page})
	c.items[page.Num] = elem
}

// evictLocked removes the least-recently-used entry. Must be called with mu held.
func (c *Cache) evictLocked() {
	back := c.list.Back()
	if back == nil {
		return
	}
	c.list.Remove(back)
	delete(c.items, back.Value.(*lruEntry).pageNum)
}

// Remove deletes a specific page from the cache.
func (c *Cache) Remove(pageNum uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.items[pageNum]; ok {
		c.list.Remove(elem)
		delete(c.items, pageNum)
	}
}

// Clear empties the cache and resets statistics.
func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.list.Init()
	c.items = make(map[uint32]*list.Element)
	c.hits = 0
	c.misses = 0
}

// Size returns the number of pages currently in the cache.
func (c *Cache) Size() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.list.Len()
}

// Stats returns cache hit and miss counts.
func (c *Cache) Stats() (hits, misses int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses
}

// SetCapacity resizes the cache, evicting LRU entries if needed.
func (c *Cache) SetCapacity(capacity int) {
	if capacity < 0 {
		capacity = (-capacity * 1024) / DefaultPageSize
	}
	if capacity <= 0 {
		capacity = 2000
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.capacity = capacity
	for c.list.Len() > c.capacity {
		c.evictLocked()
	}
}
