//go:build SVDB_ENABLE_CGO_DS

package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "cache.h"
#include <stdlib.h>
*/
import "C"
import "unsafe"

// Cache is an LRU page cache backed by C++ implementation.
// Get promotes entries to most-recently-used;
// when capacity is exceeded the least-recently-used entry is evicted.
//
// Capacity follows the SQLite convention:
// - Positive values: page counts
// - Negative values: cache size in KiB (e.g., -2000 means 2000 KiB)
// - Zero: defaults to 2000 pages
type Cache struct {
	cCache unsafe.Pointer // *C.svdb_cache_t
}

// NewCache creates an LRU cache.
// Capacity follows the SQLite convention:
// - Positive values: page counts
// - Negative values: cache size in KiB (e.g., -2000 means 2000 KiB)
// - Zero: defaults to 2000 pages
func NewCache(capacity int) *Cache {
	return &Cache{cCache: unsafe.Pointer(C.svdb_cache_create(C.int(capacity)))}
}

// getCcache converts the unsafe.Pointer to C cache pointer
func getCcache(c *Cache) *C.svdb_cache_t {
	if c == nil || c.cCache == nil {
		return nil
	}
	return (*C.svdb_cache_t)(c.cCache)
}

// Get returns the page for pageNum and promotes it to most-recently-used.
// Returns nil, false if page not found.
func (c *Cache) Get(pageNum uint32) (*Page, bool) {
	cCache := getCcache(c)
	if cCache == nil {
		return nil, false
	}

	var pageData *C.uint8_t
	var pageSize C.size_t

	if C.svdb_cache_get(cCache, C.uint32_t(pageNum), &pageData, &pageSize) == 0 {
		return nil, false
	}

	page := &Page{
		Num:  pageNum,
		Data: make([]byte, pageSize),
	}
	copy(page.Data, C.GoBytes(unsafe.Pointer(pageData), C.int(pageSize)))
	return page, true
}

// Set inserts or updates a page in the cache.
// Evicts the LRU entry if the cache is full.
func (c *Cache) Set(page *Page) {
	if page == nil || len(page.Data) == 0 {
		return
	}
	C.svdb_cache_set(
		getCcache(c),
		C.uint32_t(page.Num),
		(*C.uint8_t)(unsafe.Pointer(&page.Data[0])),
		C.size_t(len(page.Data)),
	)
}

// Remove deletes a specific page from the cache.
func (c *Cache) Remove(pageNum uint32) {
	C.svdb_cache_remove(getCcache(c), C.uint32_t(pageNum))
}

// Clear empties the cache and resets statistics.
func (c *Cache) Clear() {
	C.svdb_cache_clear(getCcache(c))
}

// Size returns the number of pages currently in the cache.
func (c *Cache) Size() int {
	return int(C.svdb_cache_size(getCcache(c)))
}

// Stats returns cache hit and miss counts.
func (c *Cache) Stats() (hits, misses int) {
	var cHits, cMisses C.int
	C.svdb_cache_stats(getCcache(c), &cHits, &cMisses)
	return int(cHits), int(cMisses)
}

// SetCapacity resizes the cache, evicting LRU entries if needed.
// Follows the same sign convention as NewCache.
func (c *Cache) SetCapacity(capacity int) {
	C.svdb_cache_set_capacity(getCcache(c), C.int(capacity))
}

// Close frees the C++ cache resources.
func (c *Cache) Close() error {
	if c.cCache != nil {
		C.svdb_cache_destroy(getCcache(c))
		c.cCache = nil
	}
	return nil
}
