package pb

/*
#cgo LDFLAGS: -L${SRCDIR}/../../../.build/cmake/lib -lsvdb -lstdc++
#cgo CXXFLAGS: -std=c++17
#include <stdlib.h>

// Forward declare C++ functions we need
extern void* SVDB_PB_VFS_Create();
extern void SVDB_PB_VFS_Destroy(void* vfs);
extern void* SVDB_PB_VFS_Open(void* vfs, const char* path, int flags);
extern int SVDB_PB_VFS_Close(void* file);
extern long long SVDB_PB_VFS_Read(void* file, unsigned char* buf, long long len, long long offset);
extern long long SVDB_PB_VFS_Write(void* file, const unsigned char* data, long long len, long long offset);
extern int SVDB_PB_VFS_Sync(void* file);
extern long long SVDB_PB_VFS_GetSize(void* file);
extern int SVDB_PB_VFS_Truncate(void* file, long long size);

// Open flags
#define SVDB_PB_OPEN_READONLY   0x00000001
#define SVDB_PB_OPEN_WRITEONLY  0x00000002
#define SVDB_PB_OPEN_READWRITE  0x00000003
#define SVDB_PB_OPEN_CREATE     0x00000004
#define SVDB_PB_OPEN_EXCLUSIVE  0x00000008
#define SVDB_PB_OPEN_TRUNC      0x00000010
#define SVDB_PB_OPEN_APPEND     0x00000020
*/
import "C"
import (
	"errors"
	"runtime"
	"sync"
	"unsafe"
)

// File interface for database file operations
type File interface {
	ReadAt(p []byte, off int64) (n int, err error)
	WriteAt(p []byte, off int64) (n int, err error)
	Sync() error
	Close() error
	Lock(lockType LockType) error
	Unlock() error
	Size() (int64, error)
	Truncate(size int64) error
}

// VFS errors
var (
	ErrInvalidFlag  = errors.New("invalid file flag")
	ErrNotLocked    = errors.New("file not locked")
	ErrLocked       = errors.New("file already locked")
	ErrFileNotFound = errors.New("file not found")
)

// File flags
const (
	O_CREATE  = int(C.SVDB_PB_OPEN_CREATE)
	O_EXCL    = int(C.SVDB_PB_OPEN_EXCLUSIVE)
	O_TRUNC   = int(C.SVDB_PB_OPEN_TRUNC)
	O_RDWR    = int(C.SVDB_PB_OPEN_READWRITE)
	O_RDONLY  = int(C.SVDB_PB_OPEN_READONLY)
	O_WRONLY  = int(C.SVDB_PB_OPEN_WRITEONLY)
)

// LockType represents the type of lock on a file
type LockType int

const (
	LockNone LockType = iota
	LockShared
	LockReserved
	LockExclusive
)

// fileImpl implements File interface using C++ VFS
type fileImpl struct {
	handle  unsafe.Pointer
	vfs     *VFS
	path    string
	mu      sync.Mutex
	curLock LockType
}

// VFS represents a virtual file system
type VFS struct {
	handle unsafe.Pointer
}

// NewVFS creates a new VFS instance
func NewVFS() *VFS {
	v := &VFS{
		handle: C.SVDB_PB_VFS_Create(),
	}
	runtime.SetFinalizer(v, func(v *VFS) {
		if v.handle != nil {
			C.SVDB_PB_VFS_Destroy(v.handle)
			v.handle = nil
		}
	})
	return v
}

// Destroy frees the VFS resources
func (v *VFS) Destroy() {
	if v.handle != nil {
		C.SVDB_PB_VFS_Destroy(v.handle)
		v.handle = nil
	}
	runtime.SetFinalizer(v, nil)
}

// openFile opens a file using the VFS
func (v *VFS) openFile(path string, flag int) (File, error) {
	if v.handle == nil {
		return nil, errors.New("VFS not initialized")
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	cFlags := C.int(flag)
	handle := C.SVDB_PB_VFS_Open(v.handle, cPath, cFlags)
	if handle == nil {
		return nil, ErrFileNotFound
	}

	f := &fileImpl{
		handle:  handle,
		vfs:     v,
		path:    path,
		curLock: LockNone,
	}

	runtime.SetFinalizer(f, func(f *fileImpl) {
		if f.handle != nil {
			C.SVDB_PB_VFS_Close(f.handle)
			f.handle = nil
		}
	})

	return f, nil
}

// OpenFile opens a file using the default VFS
func OpenFile(uri string, flag int) (File, error) {
	// Use a global default VFS
	defaultVFS := getDefaultVFS()
	return defaultVFS.openFile(uri, flag)
}

var (
	defaultVFS   *VFS
	defaultVFSMu sync.Mutex
)

func getDefaultVFS() *VFS {
	defaultVFSMu.Lock()
	defer defaultVFSMu.Unlock()

	if defaultVFS == nil {
		defaultVFS = NewVFS()
	}
	return defaultVFS
}

// ReadAt reads len(p) bytes into p at offset off
func (f *fileImpl) ReadAt(p []byte, off int64) (n int, err error) {
	if f.handle == nil || len(p) == 0 {
		return 0, errors.New("invalid file handle or empty buffer")
	}

	n64 := C.SVDB_PB_VFS_Read(f.handle, (*C.uchar)(unsafe.Pointer(&p[0])), C.longlong(len(p)), C.longlong(off))
	if n64 < 0 {
		return 0, errors.New("read error")
	}
	return int(n64), nil
}

// WriteAt writes len(p) bytes from p at offset off
func (f *fileImpl) WriteAt(p []byte, off int64) (n int, err error) {
	if f.handle == nil || len(p) == 0 {
		return 0, errors.New("invalid file handle or empty buffer")
	}

	n64 := C.SVDB_PB_VFS_Write(f.handle, (*C.uchar)(unsafe.Pointer(&p[0])), C.longlong(len(p)), C.longlong(off))
	if n64 < 0 {
		return 0, errors.New("write error")
	}
	return int(n64), nil
}

// Sync flushes the file to disk
func (f *fileImpl) Sync() error {
	if f.handle == nil {
		return errors.New("invalid file handle")
	}

	ret := C.SVDB_PB_VFS_Sync(f.handle)
	if ret != 0 {
		return errors.New("sync error")
	}
	return nil
}

// Close closes the file
func (f *fileImpl) Close() error {
	if f.handle == nil {
		return errors.New("invalid file handle")
	}

	ret := C.SVDB_PB_VFS_Close(f.handle)
	f.handle = nil
	runtime.SetFinalizer(f, nil)

	if ret != 0 {
		return errors.New("close error")
	}
	return nil
}

// Lock locks the file with the specified lock type
func (f *fileImpl) Lock(lockType LockType) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if lockType == LockNone {
		return f.Unlock()
	}

	if f.curLock >= lockType {
		return nil
	}

	// C++ VFS doesn't have locking yet - stub for now
	f.curLock = lockType
	return nil
}

// Unlock unlocks the file
func (f *fileImpl) Unlock() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.curLock == LockNone {
		return nil
	}

	f.curLock = LockNone
	return nil
}

// Size returns the file size
func (f *fileImpl) Size() (int64, error) {
	if f.handle == nil {
		return 0, errors.New("invalid file handle")
	}

	size := C.SVDB_PB_VFS_GetSize(f.handle)
	if size < 0 {
		return 0, errors.New("get size error")
	}
	return int64(size), nil
}

// Truncate truncates the file to size
func (f *fileImpl) Truncate(size int64) error {
	if f.handle == nil {
		return errors.New("invalid file handle")
	}

	ret := C.SVDB_PB_VFS_Truncate(f.handle, C.longlong(size))
	if ret != 0 {
		return errors.New("truncate error")
	}
	return nil
}

// Path returns the file path
func (f *fileImpl) Path() string {
	return f.path
}
