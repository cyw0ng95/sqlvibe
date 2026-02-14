package ds

import (
	"sync"
)

type Cache struct {
	mu      sync.RWMutex
	pages   map[uint32]*Page
	maxSize int
	hits    int
	misses  int
}

func NewCache(maxSize int) *Cache {
	return &Cache{
		pages:   make(map[uint32]*Page),
		maxSize: maxSize,
		hits:    0,
		misses:  0,
	}
}

func (c *Cache) Get(pageNum uint32) (*Page, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	page, ok := c.pages[pageNum]
	if ok {
		c.hits++
		return page, true
	}
	c.misses++
	return nil, false
}

func (c *Cache) Set(page *Page) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.pages) >= c.maxSize {
		c.evict()
	}
	c.pages[page.Num] = page
}

func (c *Cache) evict() {
	for num := range c.pages {
		delete(c.pages, num)
		return
	}
}

func (c *Cache) Remove(pageNum uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.pages, pageNum)
}

func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pages = make(map[uint32]*Page)
	c.hits = 0
	c.misses = 0
}

func (c *Cache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.pages)
}

func (c *Cache) Stats() (hits, misses int) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hits, c.misses
}
