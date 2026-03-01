package DS

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/DS
#include "overflow.h"
#include "btree.h"
#include <stdlib.h>
#include <stdint.h>

// Forward declarations of Go callbacks
extern int goPageRead(void* user_data, uint32_t page_num, uint8_t** page_data, size_t* page_size);
extern int goPageWrite(void* user_data, uint32_t page_num, uint8_t* page_data, size_t page_size);
extern int goPageAllocate(void* user_data, uint32_t* page_num);
extern int goPageFree(void* user_data, uint32_t page_num);

// Helper functions that use Go callbacks
static inline uint32_t svdb_overflow_write_chain_go(uintptr_t pm_id,
                                                     const uint8_t* payload, size_t payload_len) {
    svdb_page_manager_t cgo_pm;
    cgo_pm.user_data = (void*)pm_id;
    cgo_pm.read_page = (read_page_fn)goPageRead;
    cgo_pm.write_page = (write_page_fn)goPageWrite;
    cgo_pm.allocate_page = (allocate_page_fn)goPageAllocate;
    cgo_pm.free_page = (free_page_fn)goPageFree;
    uint32_t first_page = 0;
    if (svdb_overflow_write_chain(&cgo_pm, payload, payload_len, &first_page) != 1) {
        return 0;
    }
    return first_page;
}

static inline uint8_t* svdb_overflow_read_chain_go(uintptr_t pm_id,
                                                    uint32_t first_page, size_t total_size,
                                                    size_t* out_len) {
    svdb_page_manager_t cgo_pm;
    cgo_pm.user_data = (void*)pm_id;
    cgo_pm.read_page = (read_page_fn)goPageRead;
    cgo_pm.write_page = (write_page_fn)goPageWrite;
    cgo_pm.allocate_page = (allocate_page_fn)goPageAllocate;
    cgo_pm.free_page = (free_page_fn)goPageFree;
    uint8_t* out_buf = NULL;
    size_t len = 0;
    if (svdb_overflow_read_chain(&cgo_pm, first_page, total_size, &out_buf, &len) != 1) {
        return NULL;
    }
    *out_len = len;
    return out_buf;
}

static inline int svdb_overflow_free_chain_go(uintptr_t pm_id, uint32_t first_page) {
    svdb_page_manager_t cgo_pm;
    cgo_pm.user_data = (void*)pm_id;
    cgo_pm.read_page = (read_page_fn)goPageRead;
    cgo_pm.write_page = (write_page_fn)goPageWrite;
    cgo_pm.allocate_page = (allocate_page_fn)goPageAllocate;
    cgo_pm.free_page = (free_page_fn)goPageFree;
    return svdb_overflow_free_chain(&cgo_pm, first_page);
}

static inline size_t svdb_overflow_chain_length_go(uintptr_t pm_id, uint32_t first_page) {
    svdb_page_manager_t cgo_pm;
    cgo_pm.user_data = (void*)pm_id;
    cgo_pm.read_page = (read_page_fn)goPageRead;
    cgo_pm.write_page = (write_page_fn)goPageWrite;
    cgo_pm.allocate_page = (allocate_page_fn)goPageAllocate;
    cgo_pm.free_page = (free_page_fn)goPageFree;
    size_t len = 0;
    if (svdb_overflow_chain_length(&cgo_pm, first_page, &len) != 1) {
        return (size_t)-1;
    }
    return len;
}
*/
import "C"
import (
	"fmt"
	"runtime"
	"sync"
	"unsafe"

	"github.com/cyw0ng95/sqlvibe/internal/SF/util"
)

// Global registry for PageManager pointers
var (
	pmRegistry = struct {
		sync.Mutex
		nextID uintptr
		m      map[uintptr]*PageManager
	}{
		nextID: 1,
		m:      make(map[uintptr]*PageManager),
	}
)

// registerPageManager registers a PageManager and returns an ID
func registerPageManager(pm *PageManager) uintptr {
	pmRegistry.Lock()
	defer pmRegistry.Unlock()
	id := pmRegistry.nextID
	pmRegistry.nextID++
	pmRegistry.m[id] = pm
	return id
}

// unregisterPageManager removes a PageManager from the registry
func unregisterPageManager(id uintptr) {
	pmRegistry.Lock()
	defer pmRegistry.Unlock()
	delete(pmRegistry.m, id)
}

// getPageManager retrieves a PageManager from the registry
func getPageManager(id uintptr) *PageManager {
	pmRegistry.Lock()
	defer pmRegistry.Unlock()
	return pmRegistry.m[id]
}

//export goPageRead
func goPageRead(userData unsafe.Pointer, pageNum C.uint32_t, pageData **C.uint8_t, pageSize *C.size_t) C.int {
	id := uintptr(userData)
	pm := getPageManager(id)
	if pm == nil {
		return C.int(0)
	}
	page, err := pm.ReadPage(uint32(pageNum))
	if err != nil {
		return C.int(0)
	}
	*pageData = (*C.uint8_t)(unsafe.Pointer(&page.Data[0]))
	*pageSize = C.size_t(len(page.Data))
	return C.int(1)
}

//export goPageWrite
func goPageWrite(userData unsafe.Pointer, pageNum C.uint32_t, pageData *C.uint8_t, pageSize C.size_t) C.int {
	id := uintptr(userData)
	pm := getPageManager(id)
	if pm == nil {
		return C.int(0)
	}
	page, err := pm.ReadPage(uint32(pageNum))
	if err != nil {
		return C.int(0)
	}
	copy(page.Data, C.GoBytes(unsafe.Pointer(pageData), C.int(pageSize)))
	if err := pm.WritePage(page); err != nil {
		return C.int(0)
	}
	return C.int(1)
}

//export goPageAllocate
func goPageAllocate(userData unsafe.Pointer, pageNum *C.uint32_t) C.int {
	id := uintptr(userData)
	pm := getPageManager(id)
	if pm == nil {
		return C.int(0)
	}
	pn, err := pm.AllocatePage()
	if err != nil {
		return C.int(0)
	}
	*pageNum = C.uint32_t(pn)
	return C.int(1)
}

//export goPageFree
func goPageFree(userData unsafe.Pointer, pageNum C.uint32_t) C.int {
	id := uintptr(userData)
	pm := getPageManager(id)
	if pm == nil {
		return C.int(0)
	}
	if err := pm.FreePage(uint32(pageNum)); err != nil {
		return C.int(0)
	}
	return C.int(1)
}

// WriteOverflowChain writes a large payload across multiple overflow pages
func (om *OverflowManager) WriteOverflowChain(payload []byte) (uint32, error) {
	util.AssertNotNil(payload, "payload")

	if len(payload) == 0 {
		return 0, nil
	}

	util.Assert(om.pm.PageSize() > int(OverflowPageHeaderSize), "page size too small for overflow: %d", om.pm.PageSize())

	// Register PageManager and get ID
	pmID := registerPageManager(om.pm)
	defer unregisterPageManager(pmID)

	// Pin payload
	var pinner runtime.Pinner
	if len(payload) > 0 {
		pinner.Pin(&payload[0])
	}
	defer pinner.Unpin()

	var payloadPtr *C.uint8_t
	if len(payload) > 0 {
		payloadPtr = (*C.uint8_t)(unsafe.Pointer(&payload[0]))
	}

	firstPage := C.svdb_overflow_write_chain_go(C.uintptr_t(pmID), payloadPtr, C.size_t(len(payload)))

	if firstPage == 0 {
		return 0, fmt.Errorf("C++ overflow write failed")
	}

	return uint32(firstPage), nil
}

// ReadOverflowChain reads a complete payload from an overflow chain
func (om *OverflowManager) ReadOverflowChain(firstPage uint32, totalSize int) ([]byte, error) {
	if firstPage == 0 || totalSize == 0 {
		return nil, nil
	}

	// Register PageManager and get ID
	pmID := registerPageManager(om.pm)
	defer unregisterPageManager(pmID)

	var outLen C.size_t
	outBuf := C.svdb_overflow_read_chain_go(C.uintptr_t(pmID), C.uint32_t(firstPage), C.size_t(totalSize), &outLen)

	if outBuf == nil {
		return nil, fmt.Errorf("C++ overflow read failed")
	}

	resultBuf := C.GoBytes(unsafe.Pointer(outBuf), C.int(outLen))
	C.free(unsafe.Pointer(outBuf))

	return resultBuf, nil
}

// FreeOverflowChain frees all pages in an overflow chain
func (om *OverflowManager) FreeOverflowChain(firstPage uint32) error {
	if firstPage == 0 {
		return nil
	}

	// Register PageManager and get ID
	pmID := registerPageManager(om.pm)
	defer unregisterPageManager(pmID)

	result := C.svdb_overflow_free_chain_go(C.uintptr_t(pmID), C.uint32_t(firstPage))

	if result != 1 {
		return fmt.Errorf("C++ overflow free failed")
	}

	return nil
}

// GetOverflowChainLength calculates the number of pages in an overflow chain
func (om *OverflowManager) GetOverflowChainLength(firstPage uint32) (int, error) {
	if firstPage == 0 {
		return 0, nil
	}

	// Register PageManager and get ID
	pmID := registerPageManager(om.pm)
	defer unregisterPageManager(pmID)

	result := C.svdb_overflow_chain_length_go(C.uintptr_t(pmID), C.uint32_t(firstPage))

	if result == C.size_t(^uint(0)) {
		return 0, fmt.Errorf("C++ overflow chain length failed")
	}

	return int(result), nil
}
