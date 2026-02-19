package DS

import (
	"sync"
)

type cacheItem struct {
	key   uint32
	value *Page
	prev  *cacheItem
	next  *cacheItem
	dirty bool
}

type Cache struct {
	mu      sync.RWMutex
	pages   map[uint32]*cacheItem
	maxSize int
	hits    int
	misses  int
	head    *cacheItem
	tail    *cacheItem
}

func NewCache(maxSize int) *Cache {
	if maxSize <= 0 {
		maxSize = 100
	}
	c := &Cache{
		pages:   make(map[uint32]*cacheItem),
		maxSize: maxSize,
		hits:    0,
		misses:  0,
	}
	c.head = &cacheItem{}
	c.tail = &cacheItem{}
	c.head.next = c.tail
	c.tail.prev = c.head
	return c
}

func (c *Cache) Get(pageNum uint32) (*Page, bool) {
	c.mu.RLock()
	item, ok := c.pages[pageNum]
	if ok {
		c.hits++
		c.mu.RUnlock()
		c.mu.Lock()
		c.moveToFront(item)
		c.mu.Unlock()
		return item.value, true
	}
	c.misses++
	c.mu.RUnlock()
	return nil, false
}

func (c *Cache) Set(page *Page) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if item, ok := c.pages[page.Num]; ok {
		item.value = page
		c.moveToFront(item)
		return
	}

	if len(c.pages) >= c.maxSize {
		c.evict()
	}

	item := &cacheItem{
		key:   page.Num,
		value: page,
	}
	c.pages[page.Num] = item
	c.addToFront(item)
}

func (c *Cache) SetDirty(pageNum uint32, dirty bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if item, ok := c.pages[pageNum]; ok {
		item.dirty = dirty
	}
}

func (c *Cache) IsDirty(pageNum uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if item, ok := c.pages[pageNum]; ok {
		return item.dirty
	}
	return false
}

func (c *Cache) addToFront(item *cacheItem) {
	item.prev = c.head
	item.next = c.head.next
	c.head.next.prev = item
	c.head.next = item
}

func (c *Cache) removeItem(item *cacheItem) {
	item.prev.next = item.next
	item.next.prev = item.prev
	item.prev = nil
	item.next = nil
}

func (c *Cache) moveToFront(item *cacheItem) {
	c.removeItem(item)
	c.addToFront(item)
}

func (c *Cache) evict() {
	if c.head.next == c.tail {
		return
	}
	item := c.tail.prev
	c.removeItem(item)
	delete(c.pages, item.key)
}

func (c *Cache) Remove(pageNum uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if item, ok := c.pages[pageNum]; ok {
		c.removeItem(item)
		delete(c.pages, pageNum)
	}
}

func (c *Cache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pages = make(map[uint32]*cacheItem)
	c.head.next = c.tail
	c.tail.prev = c.head
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

func (c *Cache) HitRate() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	total := c.hits + c.misses
	if total == 0 {
		return 0.0
	}
	return float64(c.hits) / float64(total)
}
