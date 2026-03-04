package pb

/*
#cgo LDFLAGS: -L${SRCDIR}/../../.build/cmake/lib -lsvdb -lstdc++
#cgo CXXFLAGS: -std=c++17
#include <stdlib.h>

extern void* SVDB_PB_VFS_Create();
extern void SVDB_PB_VFS_Destroy(void* vfs);
extern void* SVDB_PB_VFS_Open(void* vfs, const char* path, int flags);
extern int SVDB_PB_VFS_Close(void* file);
extern long long SVDB_PB_VFS_Read(void* file, unsigned char* buf, long long len, long long offset);
extern long long SVDB_PB_VFS_Write(void* file, const unsigned char* data, long long len, long long offset);
extern int SVDB_PB_VFS_Sync(void* file);
extern long long SVDB_PB_VFS_GetSize(void* file);
extern int SVDB_PB_VFS_Truncate(void* file, long long size);

#define SVDB_PB_OPEN_READONLY   0x00000001
#define SVDB_PB_OPEN_WRITEONLY  0x00000002
#define SVDB_PB_OPEN_READWRITE  0x00000003
#define SVDB_PB_OPEN_CREATE     0x00000004
#define SVDB_PB_OPEN_EXCLUSIVE  0x00000008
#define SVDB_PB_OPEN_TRUNC      0x00000010
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

// fileImpl implements File using C++ VFS
type fileImpl struct {
	handle  unsafe.Pointer
	path    string
	mu      sync.Mutex
	curLock LockType
}

// vfsWrapper wraps C++ VFS handle
type vfsWrapper struct {
	handle unsafe.Pointer
}

// OpenFile opens a file using the C++ VFS
func OpenFile(uri string, flag int) (File, error) {
	vfs := getDefaultVFS()
	return vfs.openFile(uri, flag)
}

var (
	defaultVFS   *vfsWrapper
	defaultVFSMu sync.Mutex
)

func getDefaultVFS() *vfsWrapper {
	defaultVFSMu.Lock()
	defer defaultVFSMu.Unlock()

	if defaultVFS == nil {
		handle := C.SVDB_PB_VFS_Create()
		defaultVFS = &vfsWrapper{handle: handle}
		runtime.SetFinalizer(defaultVFS, func(v *vfsWrapper) {
			if v.handle != nil {
				C.SVDB_PB_VFS_Destroy(v.handle)
				v.handle = nil
			}
		})
	}
	return defaultVFS
}

func (v *vfsWrapper) openFile(path string, flag int) (File, error) {
	if v.handle == nil {
		return nil, errors.New("VFS not initialized")
	}

	cPath := C.CString(path)
	defer C.free(unsafe.Pointer(cPath))

	cFlags := C.int(flag)
	handle := C.SVDB_PB_VFS_Open(v.handle, cPath, cFlags)
	if handle == nil {
		return nil, errors.New("file not found")
	}

	f := &fileImpl{
		handle:  handle,
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

func (f *fileImpl) ReadAt(p []byte, off int64) (n int, err error) {
	if f.handle == nil || len(p) == 0 {
		return 0, errors.New("invalid file handle")
	}

	n64 := C.SVDB_PB_VFS_Read(f.handle, (*C.uchar)(unsafe.Pointer(&p[0])), C.longlong(len(p)), C.longlong(off))
	if n64 < 0 {
		return 0, errors.New("read error")
	}
	return int(n64), nil
}

func (f *fileImpl) WriteAt(p []byte, off int64) (n int, err error) {
	if f.handle == nil || len(p) == 0 {
		return 0, errors.New("invalid file handle")
	}

	n64 := C.SVDB_PB_VFS_Write(f.handle, (*C.uchar)(unsafe.Pointer(&p[0])), C.longlong(len(p)), C.longlong(off))
	if n64 < 0 {
		return 0, errors.New("write error")
	}
	return int(n64), nil
}

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

func (f *fileImpl) Lock(lockType LockType) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if lockType == LockNone {
		return f.Unlock()
	}
	if f.curLock >= lockType {
		return nil
	}
	f.curLock = lockType
	return nil
}

func (f *fileImpl) Unlock() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.curLock = LockNone
	return nil
}

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
