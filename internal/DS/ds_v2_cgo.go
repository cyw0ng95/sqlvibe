// Package DS provides v0.11.3 C++ Data Storage layer wrappers
// These are pure type-conversion wrappers with NO business logic
// C++ owns all I/O and memory management - no callbacks to Go
package DS

/*
#cgo CXXFLAGS: -std=c++17
#cgo LDFLAGS: -L${SRCDIR}/../../../.build/cmake/lib -lsvdb -lstdc++
#include "page_manager_v2_cgo.h"
#include <stdlib.h>
*/
import "C"
import (
	"errors"
	"unsafe"
)

// PageManagerV2 provides direct C++ page management with no Go callbacks
type PageManagerV2 struct {
	ptr *C.svdb_page_manager_v2_t
}

// NewPageManagerV2 creates a new C++ PageManager with direct file I/O
func NewPageManagerV2(path string, pageSize uint32) (*PageManagerV2, error) {
	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	ptr := C.svdb_pm_v2_create(cPath, C.uint32_t(pageSize))
	if ptr == nil {
		return nil, ErrCreateFailed
	}

	return &PageManagerV2{ptr: ptr}, nil
}

// Close closes the page manager and releases all resources
func (pm *PageManagerV2) Close() {
	if pm.ptr != nil {
		C.svdb_pm_v2_close(pm.ptr)
		C.svdb_pm_v2_destroy(pm.ptr)
		pm.ptr = nil
	}
}

// ReadPage reads a page directly from C++ cache or file
func (pm *PageManagerV2) ReadPage(pageNum uint32) ([]byte, error) {
	var size C.size_t
	data := C.svdb_pm_v2_read_page(pm.ptr, C.uint32_t(pageNum), &size)
	if data == nil {
		return nil, ErrReadFailed
	}

	// Copy data to Go-managed memory
	result := C.GoBytes(unsafe.Pointer(data), C.int(size))
	C.free(unsafe.Pointer(data))
	return result, nil
}

// WritePage writes a page directly to C++ cache and file
func (pm *PageManagerV2) WritePage(pageNum uint32, data []byte) error {
	if len(data) == 0 {
		return ErrInvalidData
	}

	C.svdb_pm_v2_write_page(
		pm.ptr,
		C.uint32_t(pageNum),
		(*C.uint8_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)
	return nil
}

// AllocatePage allocates a new page from C++ freelist or extends file
func (pm *PageManagerV2) AllocatePage() (uint32, error) {
	pageNum := C.svdb_pm_v2_allocate_page(pm.ptr)
	if pageNum == 0 {
		return 0, ErrAllocateFailed
	}
	return uint32(pageNum), nil
}

// FreePage returns a page to the C++ freelist
func (pm *PageManagerV2) FreePage(pageNum uint32) {
	C.svdb_pm_v2_free_page(pm.ptr, C.uint32_t(pageNum))
}

// GetPageSize returns the configured page size
func (pm *PageManagerV2) GetPageSize() uint32 {
	return uint32(C.svdb_pm_v2_get_page_size(pm.ptr))
}

// GetPageCount returns the total number of pages
func (pm *PageManagerV2) GetPageCount() uint32 {
	return uint32(C.svdb_pm_v2_get_page_count(pm.ptr))
}

// GetFileSize returns the underlying file size in bytes
func (pm *PageManagerV2) GetFileSize() uint64 {
	return uint64(C.svdb_pm_v2_get_file_size(pm.ptr))
}

// Sync flushes all pending writes to disk
func (pm *PageManagerV2) Sync() {
	C.svdb_pm_v2_sync(pm.ptr)
}

// IsOpen returns true if the page manager is open
func (pm *PageManagerV2) IsOpen() bool {
	return C.svdb_pm_v2_is_open(pm.ptr) != 0
}

// ClearCache clears the C++ page cache
func (pm *PageManagerV2) ClearCache() {
	C.svdb_pm_v2_clear_cache(pm.ptr)
}

// GetCacheSize returns the number of pages in cache
func (pm *PageManagerV2) GetCacheSize() int {
	return int(C.svdb_pm_v2_get_cache_size(pm.ptr))
}

// ArenaV2 provides C++ arena allocation for zero-GC memory management
type ArenaV2 struct {
	ptr *C.svdb_arena_v2_t
}

// NewArenaV2 creates a new C++ arena with the given chunk size
func NewArenaV2(chunkSize int) (*ArenaV2, error) {
	ptr := C.svdb_arena_v2_create(C.size_t(chunkSize))
	if ptr == nil {
		return nil, ErrCreateFailed
	}
	return &ArenaV2{ptr: ptr}, nil
}

// Close destroys the arena and frees all memory
func (a *ArenaV2) Close() {
	if a.ptr != nil {
		C.svdb_arena_v2_destroy(a.ptr)
		a.ptr = nil
	}
}

// Alloc allocates memory from the arena
func (a *ArenaV2) Alloc(size int) []byte {
	ptr := C.svdb_arena_v2_alloc(a.ptr, C.size_t(size))
	if ptr == nil {
		return nil
	}
	// Go slice pointing to C++ memory (valid until Reset/Close)
	return unsafe.Slice((*byte)(ptr), size)
}

// Reset resets the arena (frees all allocations, keeps capacity)
func (a *ArenaV2) Reset() {
	C.svdb_arena_v2_reset(a.ptr)
}

// BytesUsed returns the number of bytes currently allocated
func (a *ArenaV2) BytesUsed() int {
	return int(C.svdb_arena_v2_bytes_used(a.ptr))
}

// BytesAllocated returns the total bytes allocated from system
func (a *ArenaV2) BytesAllocated() int {
	return int(C.svdb_arena_v2_bytes_allocated(a.ptr))
}

// CacheV2 provides C++ LRU page caching
type CacheV2 struct {
	ptr *C.svdb_cache_v2_t
}

// NewCacheV2 creates a new C++ LRU cache
// capacity > 0: max pages
// capacity < 0: cache size in KB
// capacity == 0: defaults to 2000 pages
func NewCacheV2(capacity int) (*CacheV2, error) {
	ptr := C.svdb_cache_v2_create(C.int(capacity))
	if ptr == nil {
		return nil, ErrCreateFailed
	}
	return &CacheV2{ptr: ptr}, nil
}

// Close destroys the cache
func (c *CacheV2) Close() {
	if c.ptr != nil {
		C.svdb_cache_v2_destroy(c.ptr)
		c.ptr = nil
	}
}

// Get retrieves a page from cache (returns nil if not found)
func (c *CacheV2) Get(pageNum uint32) ([]byte, error) {
	var size C.size_t
	data := C.svdb_cache_v2_get(c.ptr, C.uint32_t(pageNum), &size)
	if data == nil {
		return nil, nil // Not found is not an error
	}

	result := C.GoBytes(unsafe.Pointer(data), C.int(size))
	C.free(unsafe.Pointer(data))
	return result, nil
}

// Put stores a page in the cache
func (c *CacheV2) Put(pageNum uint32, data []byte) {
	if len(data) == 0 {
		return
	}
	C.svdb_cache_v2_put(
		c.ptr,
		C.uint32_t(pageNum),
		(*C.uint8_t)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
	)
}

// Remove removes a page from the cache
func (c *CacheV2) Remove(pageNum uint32) {
	C.svdb_cache_v2_remove(c.ptr, C.uint32_t(pageNum))
}

// Clear clears all cache entries
func (c *CacheV2) Clear() {
	C.svdb_cache_v2_clear(c.ptr)
}

// Size returns the number of pages in cache
func (c *CacheV2) Size() int {
	return int(C.svdb_cache_v2_size(c.ptr))
}

// Hits returns the number of cache hits
func (c *CacheV2) Hits() int {
	return int(C.svdb_cache_v2_hits(c.ptr))
}

// Misses returns the number of cache misses
func (c *CacheV2) Misses() int {
	return int(C.svdb_cache_v2_misses(c.ptr))
}

// HitRate returns the cache hit rate (0.0 to 1.0)
func (c *CacheV2) HitRate() float64 {
	hits := c.Hits()
	misses := c.Misses()
	total := hits + misses
	if total == 0 {
		return 0.0
	}
	return float64(hits) / float64(total)
}

// FreeListV2 provides C++ page freelist management
type FreeListV2 struct {
	ptr *C.svdb_freelist_v2_t
}

// NewFreeListV2 creates a new C++ freelist
func NewFreeListV2() (*FreeListV2, error) {
	ptr := C.svdb_freelist_v2_create()
	if ptr == nil {
		return nil, ErrCreateFailed
	}
	return &FreeListV2{ptr: ptr}, nil
}

// Close destroys the freelist
func (fl *FreeListV2) Close() {
	if fl.ptr != nil {
		C.svdb_freelist_v2_destroy(fl.ptr)
		fl.ptr = nil
	}
}

// Add adds a page to the freelist
func (fl *FreeListV2) Add(pageNum uint32) {
	C.svdb_freelist_v2_add(fl.ptr, C.uint32_t(pageNum))
}

// Allocate allocates a page from the freelist (returns 0 if empty)
func (fl *FreeListV2) Allocate() uint32 {
	return uint32(C.svdb_freelist_v2_allocate(fl.ptr))
}

// Count returns the number of free pages
func (fl *FreeListV2) Count() int {
	return int(C.svdb_freelist_v2_count(fl.ptr))
}

// Clear clears the freelist
func (fl *FreeListV2) Clear() {
	C.svdb_freelist_v2_clear(fl.ptr)
}

// Error definitions
var (
	ErrCreateFailed    = errors.New("DS: failed to create C++ object")
	ErrReadFailed      = errors.New("DS: failed to read page")
	ErrWriteFailed     = errors.New("DS: failed to write page")
	ErrAllocateFailed  = errors.New("DS: failed to allocate page")
	ErrInvalidData     = errors.New("DS: invalid data")
)
