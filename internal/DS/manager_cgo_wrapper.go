package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "manager.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"runtime"
	"unsafe"
)

// PageManagerInterface defines the interface for page management operations.
// Both Go PageManager and CPageManager implement this interface.
type PageManagerInterface interface {
	PageSize() int
	NumPages() uint32
	ReadPage(pageNum uint32) (*Page, error)
	WritePage(page *Page) error
	AllocatePage() (uint32, error)
	FreePage(pageNum uint32) error
	Sync() error
	Close() error
}

// CPageManager is a CGO wrapper around the C++ PageManager implementation.
// It provides file-backed page management with caching.
type CPageManager struct {
	ptr      unsafe.Pointer // *C.svdb_page_manager_t
	pageSize uint32
	dbPath   string
}

// NewCPageManager creates a new C++ PageManager.
// dbPath is the path to the SQLite database file.
// pageSize must be a power of 2 in [512, 65536].
// cachePages is the number of pages to cache (0 = default 2000).
func NewCPageManager(dbPath string, pageSize uint32, cachePages int) (*CPageManager, error) {
	if !ManagerIsValidPageSize(pageSize) {
		return nil, ErrInvalidPageSize
	}

	cPath := C.CString(dbPath)
	defer C.free(unsafe.Pointer(cPath))

	cCachePages := C.int(cachePages)
	ptr := C.svdb_page_manager_create(cPath, C.uint32_t(pageSize), cCachePages)
	if ptr == nil {
		return nil, ErrPageManagerCreate
	}

	pm := &CPageManager{
		ptr:      unsafe.Pointer(ptr),
		pageSize: pageSize,
		dbPath:   dbPath,
	}

	runtime.SetFinalizer(pm, func(p *CPageManager) {
		if p.ptr != nil {
			C.svdb_page_manager_destroy((*C.svdb_page_manager)(p.ptr))
			p.ptr = nil
		}
	})

	return pm, nil
}

// destroy frees the C++ PageManager resources.
func (pm *CPageManager) destroy() {
	if pm.ptr != nil {
		C.svdb_page_manager_destroy((*C.svdb_page_manager)(pm.ptr))
		pm.ptr = nil
	}
}

// PageSize returns the page size in bytes.
func (pm *CPageManager) PageSize() int {
	return int(pm.pageSize)
}

// NumPages returns the current number of pages in the database.
func (pm *CPageManager) NumPages() uint32 {
	if pm.ptr == nil {
		return 0
	}
	return uint32(C.svdb_page_manager_get_num_pages((*C.svdb_page_manager)(pm.ptr)))
}

// ReadPage reads a page from the database.
// pageNum is 1-based (page 1 is the header page).
// Returns the page data or an error.
func (pm *CPageManager) ReadPage(pageNum uint32) (*Page, error) {
	if pm.ptr == nil {
		return nil, ErrPageManagerClosed
	}
	if pageNum == 0 {
		return nil, ErrInvalidPage
	}

	var pageData *C.uint8_t
	var pageSize C.size_t

	ok := C.svdb_page_manager_read(
		(*C.svdb_page_manager)(pm.ptr),
		C.uint32_t(pageNum),
		&pageData,
		&pageSize,
	)
	if ok == 0 {
		return nil, ErrPageRead
	}

	// Copy data from C to Go
	page := NewPage(pageNum, int(pm.pageSize))
	C.memcpy(unsafe.Pointer(&page.Data[0]), unsafe.Pointer(pageData), C.size_t(pm.pageSize))

	if pageNum == 1 {
		page.Type = PageType(0) // Header page
	} else {
		page.Type = PageType(page.Data[0])
	}

	return page, nil
}

// WritePage writes a page to the database.
// pageNum is 1-based.
func (pm *CPageManager) WritePage(page *Page) error {
	if pm.ptr == nil {
		return ErrPageManagerClosed
	}
	if page == nil || page.Num == 0 {
		return ErrInvalidPage
	}

	var pinner runtime.Pinner
	pinner.Pin(&page.Data[0])
	defer pinner.Unpin()

	ok := C.svdb_page_manager_write(
		(*C.svdb_page_manager)(pm.ptr),
		C.uint32_t(page.Num),
		(*C.uint8_t)(unsafe.Pointer(&page.Data[0])),
		C.size_t(len(page.Data)),
	)
	if ok == 0 {
		return ErrPageWrite
	}

	page.IsDirty = false
	return nil
}

// AllocatePage allocates a new page and returns its number (1-based).
func (pm *CPageManager) AllocatePage() (uint32, error) {
	if pm.ptr == nil {
		return 0, ErrPageManagerClosed
	}

	var pageNum C.uint32_t
	ok := C.svdb_page_manager_allocate(
		(*C.svdb_page_manager)(pm.ptr),
		&pageNum,
	)
	if ok == 0 {
		return 0, ErrPageAllocate
	}

	return uint32(pageNum), nil
}

// FreePage marks a page as free.
func (pm *CPageManager) FreePage(pageNum uint32) error {
	if pm.ptr == nil {
		return ErrPageManagerClosed
	}
	if pageNum == 0 {
		return ErrInvalidPage
	}

	ok := C.svdb_page_manager_free(
		(*C.svdb_page_manager)(pm.ptr),
		C.uint32_t(pageNum),
	)
	if ok == 0 {
		return ErrPageFree
	}

	return nil
}

// Sync flushes all pending writes to disk and updates the header.
func (pm *CPageManager) Sync() error {
	if pm.ptr == nil {
		return ErrPageManagerClosed
	}

	ok := C.svdb_page_manager_sync((*C.svdb_page_manager)(pm.ptr))
	if ok == 0 {
		return ErrPageSync
	}

	return nil
}

// Header returns the database header (page 1).
func (pm *CPageManager) Header() (*DatabaseHeader, error) {
	page, err := pm.ReadPage(1)
	if err != nil {
		return nil, err
	}
	return ParseHeader(page.Data)
}

// Close closes the PageManager and frees all resources.
func (pm *CPageManager) Close() error {
	pm.destroy()
	return nil
}

// NewCPageManagerFromPath creates a C++ PageManager from a database file path.
// This is a convenience function for creating a PageManager without going through
// the Go PB.File interface.
func NewCPageManagerFromPath(dbPath string, pageSize uint32) (*CPageManager, error) {
	return NewCPageManager(dbPath, pageSize, 0)
}

// ============================================================================
// Error Definitions (only those not already in page.go)
// ============================================================================

// ErrPageManagerCreate is returned when PageManager creation fails.
var ErrPageManagerCreate = &pageManagerError{"failed to create PageManager"}

// ErrPageManagerClosed is returned when operations are attempted on a closed PageManager.
var ErrPageManagerClosed = &pageManagerError{"PageManager is closed"}

// ErrPageRead is returned when a page read fails.
var ErrPageRead = &pageManagerError{"failed to read page"}

// ErrPageWrite is returned when a page write fails.
var ErrPageWrite = &pageManagerError{"failed to write page"}

// ErrPageAllocate is returned when page allocation fails.
var ErrPageAllocate = &pageManagerError{"failed to allocate page"}

// ErrPageFree is returned when freeing a page fails.
var ErrPageFree = &pageManagerError{"failed to free page"}

// ErrPageSync is returned when sync fails.
var ErrPageSync = &pageManagerError{"failed to sync PageManager"}

// pageManagerError represents a PageManager-related error.
type pageManagerError struct {
	msg string
}

func (e *pageManagerError) Error() string {
	return "PageManager: " + e.msg
}
