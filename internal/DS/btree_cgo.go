package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "btree.h"
#include <stdlib.h>
#include <stdint.h>

// These callbacks are exported by overflow_cgo.go and reused here.
extern int goPageRead(void* user_data, uint32_t page_num, uint8_t** page_data, size_t* page_size);
extern int goPageWrite(void* user_data, uint32_t page_num, uint8_t* page_data, size_t page_size);
extern int goPageAllocate(void* user_data, uint32_t* page_num);
extern int goPageFree(void* user_data, uint32_t page_num);

// svdb_btree_create_go creates a btree with Go page-manager callbacks.
static inline svdb_btree_t* svdb_btree_create_go(uintptr_t pm_id,
                                                   uint32_t root_page,
                                                   int is_table,
                                                   uint32_t page_size) {
    svdb_btree_config_t cfg;
    cfg.root_page = root_page;
    cfg.is_table  = is_table;
    cfg.page_size = page_size;

    svdb_page_manager_t pm;
    pm.user_data      = (void*)pm_id;
    pm.read_page      = (read_page_fn)goPageRead;
    pm.write_page     = (write_page_fn)goPageWrite;
    pm.allocate_page  = (allocate_page_fn)goPageAllocate;
    pm.free_page      = (free_page_fn)goPageFree;

    return svdb_btree_create(&cfg, &pm);
}

// svdb_btree_bsearch_go wraps the page-level binary search.
static inline int svdb_btree_bsearch_go(const uint8_t* page_data, size_t page_size,
                                          const uint8_t* key, size_t key_len,
                                          int is_table) {
    return svdb_btree_binary_search(page_data, page_size, key, key_len, is_table);
}
*/
import "C"
import (
	"fmt"
	"runtime"
	"unsafe"

	"github.com/cyw0ng95/sqlvibe/internal/SF/util"
)

// CBTree is a CGO wrapper around the C++ B-Tree implementation.
// It uses Go callbacks for page I/O so the existing Go PageManager
// is fully reused — no C++ PageManager is required.
type CBTree struct {
	ptr  unsafe.Pointer // *C.svdb_btree_t
	pmID uintptr        // PageManager registry ID (kept alive for callbacks)
	pm   *PageManager   // Go PageManager (kept alive to prevent GC)
}

// NewCBTree creates a new C++ B-Tree wrapper.
// pm is the Go PageManager, rootPage is the root page number (1-based),
// isTable indicates whether this is a table (true) or index (false) B-Tree.
func NewCBTree(pm *PageManager, rootPage uint32, isTable bool) *CBTree {
	util.AssertNotNil(pm, "PageManager")
	util.Assert(rootPage > 0, "rootPage must be > 0")

	pmID := registerPageManager(pm)
	isTableInt := 0
	if isTable {
		isTableInt = 1
	}

	ptr := unsafe.Pointer(C.svdb_btree_create_go(
		C.uintptr_t(pmID),
		C.uint32_t(rootPage),
		C.int(isTableInt),
		C.uint32_t(pm.PageSize()),
	))
	if ptr == nil {
		unregisterPageManager(pmID)
		return nil
	}

	bt := &CBTree{ptr: ptr, pmID: pmID, pm: pm}
	runtime.SetFinalizer(bt, func(b *CBTree) {
		if b.ptr != nil {
			C.svdb_btree_destroy((*C.svdb_btree_t)(b.ptr))
			b.ptr = nil
		}
		if b.pmID != 0 {
			unregisterPageManager(b.pmID)
			b.pmID = 0
		}
	})
	return bt
}

// Search looks up a key in the B-Tree.
// Returns the associated value bytes, or nil if not found.
func (bt *CBTree) Search(key []byte) ([]byte, error) {
	util.Assert(len(key) > 0, "search key cannot be empty")
	if bt.ptr == nil {
		return nil, fmt.Errorf("CBTree: nil pointer")
	}

	var outVal *C.uint8_t
	var outLen C.size_t

	found := C.svdb_btree_search(
		(*C.svdb_btree_t)(bt.ptr),
		(*C.uint8_t)(unsafe.Pointer(&key[0])),
		C.size_t(len(key)),
		&outVal,
		&outLen,
	)
	if found == 0 || outVal == nil {
		return nil, nil
	}
	result := C.GoBytes(unsafe.Pointer(outVal), C.int(outLen))
	C.free(unsafe.Pointer(outVal))
	return result, nil
}

// Insert inserts or updates a key-value pair in the B-Tree.
func (bt *CBTree) Insert(key, value []byte) error {
	util.Assert(len(key) > 0, "insert key cannot be empty")
	if bt.ptr == nil {
		return fmt.Errorf("CBTree: nil pointer")
	}

	var valPtr *C.uint8_t
	if len(value) > 0 {
		valPtr = (*C.uint8_t)(unsafe.Pointer(&value[0]))
	}

	ok := C.svdb_btree_insert(
		(*C.svdb_btree_t)(bt.ptr),
		(*C.uint8_t)(unsafe.Pointer(&key[0])),
		C.size_t(len(key)),
		valPtr,
		C.size_t(len(value)),
	)
	if ok == 0 {
		return fmt.Errorf("CBTree: insert failed")
	}
	return nil
}

// Delete removes a key from the B-Tree.
// Returns nil if the key was found and deleted, or if the key did not exist.
func (bt *CBTree) Delete(key []byte) error {
	util.Assert(len(key) > 0, "delete key cannot be empty")
	if bt.ptr == nil {
		return fmt.Errorf("CBTree: nil pointer")
	}

	C.svdb_btree_delete(
		(*C.svdb_btree_t)(bt.ptr),
		(*C.uint8_t)(unsafe.Pointer(&key[0])),
		C.size_t(len(key)),
	)
	return nil
}

// Depth returns the current depth of the B-Tree.
func (bt *CBTree) Depth() uint32 {
	if bt.ptr == nil {
		return 0
	}
	return uint32(C.svdb_btree_get_depth((*C.svdb_btree_t)(bt.ptr)))
}

// LeafCount returns the current leaf page count of the B-Tree.
func (bt *CBTree) LeafCount() uint32 {
	if bt.ptr == nil {
		return 0
	}
	return uint32(C.svdb_btree_get_leaf_count((*C.svdb_btree_t)(bt.ptr)))
}

// BinarySearchPage performs a binary search in a raw page buffer.
// Returns the cell index where key was found (or insertion point), or -1.
func BinarySearchPage(pageData []byte, key []byte, isTable bool) int {
	if len(pageData) == 0 || len(key) == 0 {
		return -1
	}
	isTableInt := 0
	if isTable {
		isTableInt = 1
	}
	return int(C.svdb_btree_bsearch_go(
		(*C.uint8_t)(unsafe.Pointer(&pageData[0])),
		C.size_t(len(pageData)),
		(*C.uint8_t)(unsafe.Pointer(&key[0])),
		C.size_t(len(key)),
		C.int(isTableInt),
	))
}
