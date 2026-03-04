package TM

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CFLAGS: -I${SRCDIR}/../../src/core/TM
#include "mvcc.h"
#include <stdlib.h>
#include <string.h>
*/
import "C"
import (
	"runtime"
	"sync"
	"unsafe"
)

// MVCCStoreCGO is a CGO wrapper around the C++ MVCC store.
// It provides multi-version concurrency control with snapshot isolation.
// Use this instead of the Go MVCCStore for better performance.
type MVCCStoreCGO struct {
	ptr  unsafe.Pointer // *C.svdb_mvcc_store_t
	mu   sync.RWMutex
}

// SnapshotCGO represents a read-consistent view of the MVCC store.
type SnapshotCGO struct {
	ptr        unsafe.Pointer // *C.svdb_mvcc_snapshot_t
	CommitID   uint64
	ActiveTxns map[uint64]bool
}

// NewMVCCStoreCGO creates a new C++ MVCC store.
func NewMVCCStoreCGO() *MVCCStoreCGO {
	store := &MVCCStoreCGO{
		ptr: unsafe.Pointer(C.svdb_mvcc_store_create()),
	}
	runtime.SetFinalizer(store, func(s *MVCCStoreCGO) {
		if s.ptr != nil {
			C.svdb_mvcc_store_destroy((*C.svdb_mvcc_store_t)(s.ptr))
			s.ptr = nil
		}
	})
	return store
}

// destroy frees the MVCC store resources.
func (s *MVCCStoreCGO) destroy() {
	if s.ptr != nil {
		C.svdb_mvcc_store_destroy((*C.svdb_mvcc_store_t)(s.ptr))
		s.ptr = nil
	}
}

// Snapshot creates a read-consistent snapshot at the current commit ID.
func (s *MVCCStoreCGO) Snapshot() *SnapshotCGO {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapPtr := C.svdb_mvcc_store_snapshot((*C.svdb_mvcc_store_t)(s.ptr))
	if snapPtr == nil {
		return nil
	}

	snap := &SnapshotCGO{
		ptr:        unsafe.Pointer(snapPtr),
		CommitID:   uint64(C.svdb_mvcc_store_commit_id((*C.svdb_mvcc_store_t)(s.ptr))),
		ActiveTxns: make(map[uint64]bool),
	}

	runtime.SetFinalizer(snap, func(s *SnapshotCGO) {
		if s.ptr != nil {
			C.svdb_mvcc_snapshot_free((*C.svdb_mvcc_snapshot_t)(s.ptr))
			s.ptr = nil
		}
	})

	return snap
}

// Free frees the snapshot resources.
func (s *SnapshotCGO) Free() {
	if s.ptr != nil {
		C.svdb_mvcc_snapshot_free((*C.svdb_mvcc_snapshot_t)(s.ptr))
		s.ptr = nil
	}
}

// Get retrieves the value for key visible under the snapshot.
// Returns (nil, false) if key not found or deleted.
func (s *MVCCStoreCGO) Get(key []byte, snap *SnapshotCGO) ([]byte, bool) {
	if s.ptr == nil || snap == nil || len(key) == 0 {
		return nil, false
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var data *C.char
	var dataLen C.size_t

	found := C.svdb_mvcc_store_get(
		(*C.svdb_mvcc_store_t)(s.ptr),
		(*C.svdb_mvcc_snapshot_t)(snap.ptr),
		(*C.char)(unsafe.Pointer(&key[0])),
		C.size_t(len(key)),
		&data,
		&dataLen,
	)

	if found == 0 || data == nil {
		return nil, false
	}

	// Copy data from C to Go
	return C.GoBytes(unsafe.Pointer(data), C.int(dataLen)), true
}

// Put stores a new version of key with the given value.
// Returns the commit ID of the new version.
func (s *MVCCStoreCGO) Put(key []byte, value []byte) uint64 {
	if s.ptr == nil || len(key) == 0 {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	var valuePtr unsafe.Pointer
	var valueLen C.size_t
	if len(value) > 0 {
		valuePtr = unsafe.Pointer(&value[0])
		valueLen = C.size_t(len(value))
	}

	return uint64(C.svdb_mvcc_store_put(
		(*C.svdb_mvcc_store_t)(s.ptr),
		(*C.char)(unsafe.Pointer(&key[0])),
		C.size_t(len(key)),
		valuePtr,
		valueLen,
	))
}

// Delete marks key as deleted.
// Returns the commit ID of the delete marker.
func (s *MVCCStoreCGO) Delete(key []byte) uint64 {
	if s.ptr == nil || len(key) == 0 {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return uint64(C.svdb_mvcc_store_delete(
		(*C.svdb_mvcc_store_t)(s.ptr),
		(*C.char)(unsafe.Pointer(&key[0])),
		C.size_t(len(key)),
	))
}

// GC removes old versions that are no longer needed.
// keepBelow: minimum commit ID to retain (except last version per key).
// Returns number of versions pruned.
func (s *MVCCStoreCGO) GC(keepBelow uint64) int {
	if s.ptr == nil {
		return 0
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return int(C.svdb_mvcc_store_gc(
		(*C.svdb_mvcc_store_t)(s.ptr),
		C.uint64_t(keepBelow),
	))
}

// CommitID returns the current commit ID.
func (s *MVCCStoreCGO) CommitID() uint64 {
	if s.ptr == nil {
		return 0
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return uint64(C.svdb_mvcc_store_commit_id((*C.svdb_mvcc_store_t)(s.ptr)))
}

// KeyCount returns the number of keys in the store.
func (s *MVCCStoreCGO) KeyCount() int {
	if s.ptr == nil {
		return 0
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	return int(C.svdb_mvcc_store_key_count((*C.svdb_mvcc_store_t)(s.ptr)))
}

// Close closes the MVCC store and frees all resources.
func (s *MVCCStoreCGO) Close() error {
	s.destroy()
	return nil
}
